
const termSql = (col, terms) => {
    const termExpression = terms.filter((t) => Array.isArray(t))
    const termStrings = terms.filter((t) => !Array.isArray(t))

    

    const termESql =`array_compact(array_construct(${termExpression.map(([t, e]) => 
      `iff(${typeof(e) == 'string' ? 
        `regexMatchString(${col}, '${e.replace(/'/g, "''").replace(/\\/g, '\\\\')}', 'i') is not null` 
        : e(col)}
        , '${t}', null)`)}))`
    const termSSql = `arrayuniq(arrayLower(arrayReplace(regexmatchall(${col}, ${`'\\b(${
      termStrings.map(t => t.replace(/'/g, "''").replace(' ', '\\s*')).join('|')
    })\\b'`.replace(/\\/g, '\\\\')}, 'i'), '\\\\s+', '', '')))`

    return `array_cat(${termESql}, ${termSSql})`
  }

function select_context(terms, table, col, part, expressions) {
  part = part || col
  var allExpressions = [
    'video_id', 
    `${termSql(col, terms)} matches`,
    `${col} context`,
    `'${part}' part`
    ].concat(expressions ? expressions : [])
  return `select ${allExpressions.join(',\n  ')} from ${table} where matches is not null and array_size(matches)>0`
}

const arraySql = (arr) => `array_construct(${arr.map(a => `'${a.replace(/'/g, "\\'")}'`).join(', ')})`

/* provide an array of terms to find in captions/descriptions/ttles and the name of the table containing video_id to search. 
 Terms param: array of string terms. Or a tuple that returns a tuple in the form: [<name to use as the match>, a function returning a boolean extpression given the name of a column]
 returns a select query that returns matches at the video granularity.
 columns: video_id, matches, context, part, offset_secconds */
function mentionsSelect(terms, video_table, parts) {
  parts = parts || ['caption', 'description', 'title', 'keyword']
  video_table = video_table || 'video_latest'

  const partToSelect = {
    caption: select_context(terms, 'mentions_select_cap', 'caption', null, ['offset_seconds::int offset_seconds']),
    title: select_context(terms, 'mentions_select_vid', 'video_title', 'title', ['null offset_seconds']),
    description: select_context(terms, 'mentions_select_vid', 'description', null, ['null offset_seconds']),
    keyword: `  (
    with mentions_select_kw as (
      select video_id, video_title, k.value::string keyword
      from mentions_select_vid, lateral flatten(input => keywords) k
    )
    ${select_context(terms, 'mentions_select_kw', 'keyword', null, ['null offset_seconds'])}
  )`
  }



  return `
(
  with mentions_select_vid as (
      select v.video_id, l.video_title, l.description, l.keywords
      from ${video_table} v
      join video_latest l on l.video_id = v.video_id
  )
  , mentions_select_cap as (
      select s.video_id, s.caption, offset_seconds from caption s
      join mentions_select_vid v on v.video_id = s.video_id -- filter via vid table
  )
  ${parts.map(p => partToSelect[p]).join('\n union all \n')}
)
`
}

module.exports = { mentionsSelect, arraySql };