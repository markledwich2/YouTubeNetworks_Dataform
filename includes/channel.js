var categories = [
    'ideology', 'media', 'lr'
]

function recCatPair(cat) {
    return [ `from_${cat}`, `to_${cat}` ]
}

function recCatColumns() {
    return categories.map(c => recCatPair(c)).join(',')
}

function periodStatsObject(toDateCol, valueCol, alias) {
  function e(periodType, periodValue) {
    return `object_construct('periodType', '${periodType}', 'periodValue',  to_varchar(${periodValue}), 'value', sum(${valueCol}))`
  }

  var periods = [1,2,7,30,365].map(d => 
      e(`d${d}`, `dateadd(d, ${d}, ${toDateCol})`))
    .concat([...Array(12).keys()].map(m => 
      e(`m`, `add_months(date_trunc('MONTH', ${toDateCol}), -${m})`)))
    .concat([...Array(2).keys()].map(y => 
      e(`y`, `dateadd(y, -${y}, date_trunc('YEAR', ${toDateCol}))`)))

  return `array_construct(${periods.join('\n,')}) as ${alias}`
}

function lastNotNull(col, partitionCol, orderCol = 'updated') {
  var cols = Array.isArray(col) ? col : [col]
  return cols.map(c => `coalesce(${c}, lag(${c}) ignore nulls over (partition by ${partitionCol} order by ${orderCol}), ${c}) ${c}`).join('\n,')
}

module.exports = { categories, recCatColumns, periodStatsObject, lastNotNull };