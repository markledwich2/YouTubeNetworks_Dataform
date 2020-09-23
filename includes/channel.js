var categories = [
    'ideology', 'media', 'lr'
]


function recCatPair(cat) {
    return [ `from_${cat}`, `to_${cat}` ]
}

function recCatColumns() {
    return categories.map(c => recCatPair(c)).join(',')
}

function periodStatsObject(agoCol, valueCol, alias) {
  function c(agoA, agoB, name) {
    return `'${name}', cast(sum(iff(${agoCol} between ${agoA} and ${agoB}, ${valueCol}, 0))/${agoB-agoA+1} as int)`
  }

  var cols = [
    c(0, 1, 'd2'),
    c(2, 3, 'd2p'),
    c(0, 6, 'd7'),
    c(7, 13, 'd7p'),
    c(0, 29, 'd30'),
    c(30, 59, 'd30p'),
    c(0, 364, 'd365'),
    c(365, 729, 'd365p')
  ]

  return `object_construct(${cols.join('\n,')}) as ${alias}`
}

module.exports = { categories, recCatColumns, periodStatsObject };