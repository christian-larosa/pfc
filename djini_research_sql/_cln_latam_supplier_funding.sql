/*
  English translation of Spanish words used by PeYa

  |   Spanish   |    English    |
  |:-----------:|:-------------:|
  | aporte      | contribution  |
  | absoluto    | absolute      |
  | aportes     | contributions |
  | proveedores | suppliers     |
*/
CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}._cln_latam_supplier_funding`
AS
SELECT
  ap.global_entity_id
  , ap.country_code
  , ap.order_id
  , ap.sku
  , ap.`Order_fulfilled_at` AS order_fulfilled_at_utc
  , ap.campaign_id
  , ap.aporte_absoluto AS abs_discount_value_lt
FROM `peya-food-and-groceries.automated_tables_reports_shared.aportes_proveedores_dmart_ar` AS ap
