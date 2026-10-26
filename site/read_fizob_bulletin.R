# Shared SQL contract with scripts/build_fizob_bulletin.py.
read_fizob_bulletin <- function(con, repo_root) {
  sql <- paste(readLines(file.path(repo_root, "sql", "fizob_bulletin.sql"),
                        encoding = "UTF-8", warn = FALSE), collapse = "\n")
  # Restrict the large Fisher view before joining calendar years.
  sql <- gsub("WHERE tn_level IN (0, 2, 4, 6)", "WHERE tn_level = 0", sql, fixed = TRUE)
  columns <- DBI::dbGetQuery(con, "DESCRIBE fizob_enriched")$column_name
  if ("methodology_version" %in% columns) {
    sql <- gsub("json_extract_string(to_json(f), '$.base_period')",
                "CAST(f.base_period AS VARCHAR)", sql, fixed = TRUE)
    sql <- gsub("json_extract_string(to_json(f), '$.methodology_version')",
                "f.methodology_version", sql, fixed = TRUE)
  }
  DBI::dbGetQuery(con, paste0("SELECT * FROM (", sql,
    ") s WHERE tn_level = 0 AND STRANA IN ('CN','IN','TR','ALL')"))
}
