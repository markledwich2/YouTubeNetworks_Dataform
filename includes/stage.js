function create(table) {
    return `create table if not exists ${table} (v Variant)`;
}

function copy(table, sourcePath, stage) {
    return `
    copy into ${table} from @${stage ? stage : 'public.yt'}/${sourcePath}
    file_format=(type=json)
`;
}
module.exports = { create, copy };