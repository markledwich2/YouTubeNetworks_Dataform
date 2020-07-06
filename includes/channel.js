var categories = [
    'ideology', 'media', 'lr'
]


function recCatPair(cat) {
    return [ `from_${cat}`, `to_${cat}` ]
}

function recCatColumns() {
    return categories.map(c => recCatPair(c)).join(',')
}

module.exports = { categories, recCatColumns };