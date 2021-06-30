const union = (tables) => tables.map(t => `select * from ${t}`).join('\nunion all ')

module.exports = { union }