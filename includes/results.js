function save_select(name, select) {
    return `copy into ` + results_path(name) + 
    ` from (${select})` + copy_options();
}

function save_table(name, table) {
    return `copy into ` + results_path(name) + 
    ` from ${table} `+ copy_options();
}

function copy_options() {
    return `
    file_format = (type='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION='NONE')
    single=true
    overwrite=true
    header=true
    `;
}

function results_path(name) {
    return `@public.yt_results/${date_dir()}/${name}.csv`;
}

function date_dir() {
    var today = new Date();
    var dd = today.getDate();
    if (dd < 10) dd = '0' + dd;
    var mm = today.getMonth() + 1; //January is 0!
    if (mm < 10) mm = '0' + mm;
    return `${today.getFullYear()}_${mm}_${dd}`;
}

module.exports = { save_select, save_table };