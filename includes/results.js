function save_select(name, select, extension = "csv.gz") {
    return `copy into ` + results_path(name, extension) + 
    ` from (${select})` + copy_options(extension);
}

function save_table(name, table, extension = "csv.gz") {
    return `copy into ` + results_path(name, extension) + 
    ` from ${table} `+ copy_options(extension);
}

function copy_options(extension) {
    var split = extension.split(".")
    var compression = "none"
    if (split.length > 1) {
        var part = split[split.length - 1]
        if(part == "gz")
            compression = "gzip"
    }
       

    var type = [0]
    if (type = "csv")
        return `
        file_format = (type='csv' FIELD_OPTIONALLY_ENCLOSED_BY='"' COMPRESSION='${compression}' NULL_IF=())
        single=true
        overwrite=true
        header=true
        `;

    throw "not implimented"
}

function results_path(name, extension) {
    return `@public.yt_results/latest/${name}.${extension}`;
}

function date_dir() {
    var today = new Date();
    var dd = today.getDate();
    if (dd < 10) dd = '0' + dd;
    var mm = today.getMonth() + 1; //January is 0!
    if (mm < 10) mm = '0' + mm;
    return `${today.getFullYear()}-${mm}-${dd}`;
}

module.exports = { save_select, save_table };