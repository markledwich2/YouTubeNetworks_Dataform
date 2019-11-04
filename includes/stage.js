function create(table) {
    return `create table if not exists ${table} (v Variant)`;
}

function copy(table, sourcePath) {
    return `
    copy into ${table} from @public.yt/${sourcePath}
    file_format=(type=json)
`;
}
module.exports = { create, copy };