# Pinned snapshot. A future version must be selected explicitly in audit code.
load_local_code_families <- function(path='metadata/code_correspondence/2026-11-20.v1.json') {
  result <- tempfile(fileext='.json')
  on.exit(unlink(result))
  status <- system2(Sys.getenv('FIZOB_PYTHON','python'),
    c('src/core/code_correspondence.py','--registry',shQuote(path),'--output',shQuote(result)))
  stopifnot(status==0L,file.exists(result))
  config <- jsonlite::read_json(result,simplifyVector=FALSE)
  config$families <- lapply(config$rules,function(x) unlist(x$children,use.names=FALSE))
  config
}
