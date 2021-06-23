var escapeRe = (r) => r.replace(/'/g, "''").replace(/\\/g, '\\\\')
var isRe = (r) => typeof(r) == "object" && r.exec != null

var termMatch = (col, e) => {
  if(typeof(e) == 'string') return `regexMatchString(${col}, '${escapeRe(`\\b${e.replace(' ', '\\s*')}\\b`)}', 'i') is not null`
  if(isRe(e)) return `regexMatchString(${col}, '${escapeRe(e.source)}', '${e.flags}') is not null`
  if(Array.isArray(e)) return e.map(e => termMatch(col, e)).join(' or ')
  if (e instanceof Function) return e(col)
	else throw `unsupported match expression: {e}`
}


/*
 returns SQL will will returns in an array of matching terms in *col*.
 
 *terms* can be on the folllowing formats. Consider the following text in the *myCol* column:
    'Milo and Otis was a movie about a cat and a dog. I feel sheepish for watching'.

	// Simple list of words. Resutls in ['dog', 'cat']. Sheep not included because this matches on word boundaries.
  // If you need a regular expression, use one of the bellow term formats
	termMatches('myCol', ['dog', 'cat', 'sheep']) 
  
  // Provide multiple words match with terms. Result ['animal', 'Milo']. This will *or* the terms
	termMatches('myCol', [['animal', ['dog', 'cat', 'sheep']], 'Milo', 'Bob']) // returns ['animal', 'Milo']
  
  // Ptofive an expression to match with terms
  termMatches('myCol', [['catsAndDogs', c => `${termMatch(c, 'cats')} and ${termMatch(c, 'dogs')`]]) // returns ['catsAndDogs']
  
  // Provide either words, or term matches with regular extpressons
  termMatches('myCol', [['dog', ['sheep', /sheep(ish)?/]]]) // returns ['dog', 'sheep']
*/
var termMatches = (col, terms) => {
    const namedTerms = terms.filter((t) => Array.isArray(t))
    const namedTermsSql = namedTerms.length ? `array_compact(array_construct(${
    	namedTerms.map(([t, e]) => `iff(${termMatch(col, e)}, '${t}', null)`)
    }))` : null
    
    const plainTerms = terms.filter((t) => !Array.isArray(t))
    const plainTermSql = plainTerms.length ? `arrayuniq(arrayLower(arrayReplace(regexmatchall(${col}, ${`'\\b(${
      plainTerms.map(t => t.replace(/'/g, "''").replace(' ', '\\s*')).join('|')
    })\\b'`.replace(/\\/g, '\\\\')}, 'i'), '\\\\s+', '', '')))` : null
    
    const arrayCatSql = (a) => a.length > 1 ?  `array_cat(${a.join(',')})` : a.join('')
    return arrayCatSql([plainTermSql, namedTermsSql].filter(t => t))
  }

function select_context(terms, table, col, part, expressions) {
  part = part || col
  var allExpressions = [
    'video_id', 
    `${termMatches(col, terms)} matches`,
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

module.exports = { mentionsSelect, arraySql, termMatch };