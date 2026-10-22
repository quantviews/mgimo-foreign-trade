# Shared SQL contract with scripts/build_fizob_bulletin.py.
read_fizob_bulletin <- function(con, repo_root) {
  sql <- paste(readLines(file.path(repo_root, "sql", "fizob_bulletin.sql"),
                        encoding = "UTF-8", warn = FALSE), collapse = "\n")
  # Bulletins need country totals; push this filter into DuckDB.
  DBI::dbGetQuery(con, paste0("SELECT * FROM (", sql,
    ") s WHERE tn_level = 0 AND STRANA IN ('CN','IN','TR')"))
}
