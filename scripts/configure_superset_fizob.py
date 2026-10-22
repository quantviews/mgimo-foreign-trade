"""Run inside Superset after backing up metadata and uploading the LOCAL snapshot.

Only configures datasets/charts. Does not calculate or rebuild trade data.
"""
import json
from copy import deepcopy

from superset.app import create_app


def configure():
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    dashboard = db.session.query(Dashboard).filter_by(slug="marcel-trade").one()
    position = json.loads(dashboard.position_json)
    metadata = json.loads(dashboard.json_metadata)
    tab_id = next(k for k, v in position.items()
                  if isinstance(v, dict) and v.get("meta", {}).get("text") == "Физобъемы")
    tab = position[tab_id]
    ancestors = tab["parents"] + [tab_id]
    old_ids = [v["meta"]["chartId"] for v in position.values()
               if isinstance(v, dict) and v.get("type") == "CHART"
               and tab_id in v.get("parents", [])]
    datasets = {}
    for name, condition in [("fizob_bulletin_countries", "tn_level=0"),
                            ("fizob_bulletin_products", "tn_level IN (0,2,4,6)")]:
        table = db.session.query(SqlaTable).filter_by(table_name=name, database_id=1).one_or_none()
        if table is None:
            table = SqlaTable(table_name=name, database_id=1, owners=dashboard.owners)
            db.session.add(table)
        table.sql = (
            "SELECT *, strftime(window_from,'%m.%Y') || ' - ' || strftime(PERIOD,'%m.%Y') AS current_window,"
            "strftime(previous_window_from,'%m.%Y') || ' - ' || strftime(previous_window_to,'%m.%Y') AS previous_window "
            f"FROM read_parquet('/data/parquet/fizob_bulletin.parquet') WHERE {condition}")
        table.description = "Общая витрина с месячными бюллетенями. yoy - изменение сглаженного fizob г/г, доля."
        table.main_dttm_col = "PERIOD"
        db.session.flush()
        table.fetch_metadata()
        labels = {"PERIOD": "Месяц", "country": "Страна-партнер",
                  "direction": "Направление", "tn_code": "Код ТН ВЭД",
                  "product": "Товарная группа", "yoy": "Физобъем, г/г"}
        labels.update({
            "current_window": "Текущие 12 месяцев", "previous_window": "Предыдущие 12 месяцев",
            "value_12m_usd": "Стоимость, долл.", "previous_value_12m_usd": "База, долл.",
            "months_with_records": "Месяцев с записями / 12",
            "previous_months_with_records": "Месяцев в базе / 12",
            "last_source_month": "Последний месяц записей",
        })
        for column in table.columns:
            if column.column_name in labels:
                column.verbose_name = labels[column.column_name]
        datasets[name] = table

    def eq(subject, value):
        return {"expressionType": "SIMPLE", "subject": subject, "operator": "==",
                "comparator": value, "clause": "WHERE", "sqlExpression": None}

    # MAX is a scalar selector, not a cross-country/product index aggregation.
    metric = {"expressionType": "SQL", "sqlExpression": "MAX(yoy)",
              "label": "Физобъем, г/г", "hasCustomLabel": True}
    common = {
        "viz_type": "echarts_timeseries_line", "x_axis": "PERIOD",
        "granularity_sqla": "PERIOD", "time_grain_sqla": "P1M",
        "time_range": "No filter", "metrics": [metric],
        "groupby": ["direction"], "row_limit": 10000,
        "order_desc": False, "x_axis_sort_asc": True,
        # This Superset version always adds All/Inverse to chart legends.
        # Use the shared direction key in the tab header instead.
        "show_legend": False, "legendOrientation": "bottom", "legendType": "plain",
        "rich_tooltip": True, "showTooltipTotal": False,
        "y_axis_format": "+.1%", "y_axis_title": "",
        "x_axis_time_format": "%m.%Y", "tooltipTimeFormat": "%B %Y",
        "color_scheme": "mgimo", "truncateYAxis": False,
        "show_value": False, "markerEnabled": False,
        "annotation_layers": [],
        "extra_form_data": {}, "dashboards": [dashboard.id],
    }
    charts = []

    def chart(name, dataset, params):
        from superset.charts.schemas import AnnotationLayerSchema, ChartDataQueryContextSchema

        for layer in params.get("annotation_layers", []):
            AnnotationLayerSchema().load(layer)
        ChartDataQueryContextSchema().load({
            "datasource": {"id": dataset.id, "type": "table"},
            "queries": [{
                "columns": params.get("groupby", params.get("all_columns", [])),
                "metrics": params.get("metrics", []),
                "annotation_layers": params.get("annotation_layers", []),
                "granularity": "PERIOD", "time_range": "No filter",
                "extras": {"time_grain_sqla": "P1M"}, "row_limit": 10000,
            }],
            "result_format": "json", "result_type": "full",
        })
        s = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
        if s is None:
            s = Slice(slice_name=name, owners=dashboard.owners)
            db.session.add(s)
        s.datasource_id = dataset.id
        s.datasource_type = "table"
        s.viz_type = params["viz_type"]
        db.session.flush()
        params.update(datasource=f"{dataset.id}__table", slice_id=s.id)
        s.params = json.dumps(params, ensure_ascii=False)
        # Let Superset regenerate requests from the current form data.
        s.query_context = None
        charts.append(s)
        return s

    total = datasets["fizob_bulletin_countries"]
    products = datasets["fizob_bulletin_products"]
    chart("Выбранная страна и товар", products, {
        "viz_type": "table", "query_mode": "aggregate",
        "groupby": ["country", "product"], "metrics": [],
        "row_limit": 1, "page_length": 0, "include_search": False,
        "show_cell_bars": False, "time_range": "No filter",
        "granularity_sqla": "PERIOD", "adhoc_filters": [],
        "extra_form_data": {}, "dashboards": [dashboard.id],
    })
    chart("Масштаб торговли и прошлогодняя база: фактические записи", products, {
        "viz_type": "table", "query_mode": "aggregate", "groupby": ["direction"],
        # FIRST retains NULL at the latest month. ARG_MAX would backfill a
        # non-null value from an older month, silently mismatching the window.
        "metrics": [{
            "expressionType": "SQL", "sqlExpression": f"FIRST({column} ORDER BY PERIOD DESC)",
            "label": label, "hasCustomLabel": True,
        } for column, label in [
            ("current_window", "Текущие 12 месяцев"), ("value_12m_usd", "Стоимость, долл."),
            ("previous_window", "Предыдущие 12 месяцев"), ("previous_value_12m_usd", "База, долл."),
            ("months_with_records", "Месяцев с записями / 12"),
            ("previous_months_with_records", "Месяцев в базе / 12"),
            ("strftime(last_source_month,'%m.%Y')", "Последний месяц записей"),
        ]],
        "row_limit": 2, "page_length": 0, "include_search": False,
        "time_range": "No filter", "granularity_sqla": "PERIOD", "adhoc_filters": [],
        "column_config": {"Стоимость, долл.": {"d3NumberFormat": ",.2f"},
                          "База, долл.": {"d3NumberFormat": ",.2f"}},
        "extra_form_data": {}, "dashboards": [dashboard.id],
    })
    for code, label in [("ИМ", "Импорт России"), ("ЭК", "Экспорт России")]:
        params = deepcopy(common)
        params["groupby"] = ["direction"]
        params["metrics"] = [{
            "expressionType": "SQL",
            "sqlExpression": "CASE WHEN COUNT(DISTINCT country || product)=1 THEN MAX(yoy) END",
            "label": "Физобъем, г/г", "hasCustomLabel": True,
        }]
        params["adhoc_filters"] = [eq("NAPR", code)]
        chart(f"{label}: выбранная товарная группа, г/г", products, params)
    params = {
        "viz_type": "table", "query_mode": "raw", "all_columns":
            ["PERIOD", "country", "direction", "tn_code", "product", "yoy"],
        "order_by_cols": ['["PERIOD", false]'], "row_limit": 1000,
        "page_length": 24, "include_search": True, "time_range": "No filter",
        "granularity_sqla": "PERIOD", "adhoc_filters": [],
        "column_config": {"yoy": {"d3NumberFormat": "+.1%"}},
        "extra_form_data": {}, "dashboards": [dashboard.id],
    }
    chart("Товарная группа: помесячные значения", products, params)
    # Remove only the layout descendants of this tab; old charts remain saved.
    for key in list(position):
        value = position[key]
        if isinstance(value, dict) and tab_id in value.get("parents", []):
            del position[key]
    tab["children"] = []

    def row(suffix, items):
        rid = "ROW-FIZOB-" + suffix
        position[rid] = {"id": rid, "type": "ROW", "parents": ancestors,
                         "children": [], "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        tab["children"].append(rid)
        for cid, item in items:
            item.update(id=cid, parents=ancestors + [rid])
            position[cid] = item
            position[rid]["children"].append(cid)

    row("HELP", [("MARKDOWN-FIZOB-HELP", {
        "type": "MARKDOWN", "children": [], "meta": {"width": 12, "height": 22,
        "code": "### Физические объемы · изменение год к году, %\n"
        "**Импорт России - синий · Экспорт России - красный.** "
        "Индекс сглажен за 12 месяцев и объединяет товары с разными физическими единицами. "
        "Выберите страну или **Все страны в базе**, затем товар по коду или названию. "
        "По умолчанию показаны **Все страны в базе** и **Все товары**. "
        "Доступна детализация по кодам 2/4/6 знаков.\n\n"
        "Таблица показывает стоимость по имеющимся фактическим записям за последние 12 месяцев "
        "выбранного периода и предыдущие 12. Малая база может давать большой темп. "
        "Наличие всех месяцев не гарантирует полноту охвата стран."}})])
    for suffix, group, width, height in [
        ("CONTEXT", charts[:1], 12, 18), ("SCALE", charts[1:2], 12, 27),
        ("PRODUCT", charts[2:4], 6, 48), ("TABLE", charts[4:], 12, 45)]:
        row(suffix, [(f"CHART-FIZOB-{s.id}", {"type": "CHART", "children": [],
             "meta": {"chartId": s.id, "sliceName": s.slice_name,
                      "uuid": str(s.uuid), "width": width, "height": height}})
             for s in group])
    new_ids = [s.id for s in charts]
    detail_ids = new_ids
    dashboard.slices = [s for s in dashboard.slices if s.id not in old_ids + new_ids] + charts
    filters = [f for f in metadata.get("native_filter_configuration", [])
               if not f["id"].startswith("NATIVE_FILTER-FIZOB-") and f.get("type") != "DIVIDER"]
    for f in filters:
        scope = f.setdefault("scope", {})
        scope["excluded"] = sorted(set(scope.get("excluded", []) + new_ids))
        if "chartsInScope" in f:
            f["chartsInScope"] = [i for i in f["chartsInScope"] if i not in new_ids]
    def native(suffix, name, kind, dataset, column, values, ids, parents=None):
        fid = "NATIVE_FILTER-FIZOB-" + suffix
        extra = ({"time_range": values} if kind == "filter_time" else
                 {"filters": [{"col": column, "op": "IN", "val": values}]})
        f = {"id": fid, "type": "NATIVE_FILTER", "name": name, "filterType": kind,
             "targets": [{}] if kind == "filter_time" else
                 [{"datasetId": dataset.id, "column": {"name": column}}],
             "defaultDataMask": {"extraFormData": extra,
                 "filterState": {"value": values}, "ownState": {}},
             "controlValues": {"multiSelect": False, "searchAllOptions": True,
                 "enableEmptyFilter": True, "defaultToFirstItem": False,
                 "inverseSelection": False},
             "cascadeParentIds": parents or [],
             "scope": {"rootPath": [tab_id], "excluded": [i for i in new_ids if i not in ids]},
             "chartsInScope": ids, "tabsInScope": [tab_id]}
        filters.append(f)
        return fid
    # Latest 60 published months; avoid wall-clock-relative dates on a lagged source.
    latest = total.database.get_df(f"SELECT max(PERIOD) AS d FROM ({total.sql}) s").iloc[0, 0]
    from dateutil.relativedelta import relativedelta
    time_range = f"{(latest - relativedelta(months=59)):%Y-%m-%d} : {(latest + relativedelta(months=1)):%Y-%m-%d}"
    native("PERIOD-60M-V2", "Физобъемы: период", "filter_time", total, None, time_range, new_ids)
    # New identity invalidates saved China selections from the previous default.
    # Superset merges persisted data masks over defaults by filter ID.
    country_id = native("COUNTRY-ALL-V2", "Страна / все страны в базе", "filter_select", products,
                        "country", ["Все страны в базе"], detail_ids)
    # Version the product identity as well: old saved car selections must not
    # override the new all-products landing view.
    native("PRODUCT-ALL-V2", "Все товары / группа (2, 4 или 6 знаков)", "filter_select", products,
           "product", ["Все товары"], detail_ids, [country_id])
    metadata["native_filter_configuration"] = filters
    metadata.setdefault("label_colors", {}).update({"Импорт": "#003d7a", "Экспорт": "#e74c3c"})
    dashboard.position_json = json.dumps(position, ensure_ascii=False)
    dashboard.json_metadata = json.dumps(metadata, ensure_ascii=False)
    db.session.commit()
    print(json.dumps({"charts": new_ids, "datasets": {k: v.id for k, v in datasets.items()},
                      "period": time_range}, ensure_ascii=False))


if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        configure()
