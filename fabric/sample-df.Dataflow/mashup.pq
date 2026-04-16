[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "poc_data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "id", DestinationColumnName = "id"], [SourceColumnName = "bu_intersection", DestinationColumnName = "bu_intersection"], [SourceColumnName = "cust_covadex_status", DestinationColumnName = "cust_covadex_status"], [SourceColumnName = "daas_bu_owner", DestinationColumnName = "daas_bu_owner"], [SourceColumnName = "cust_car_brand", DestinationColumnName = "cust_car_brand"], [SourceColumnName = "cust_loan_amount", DestinationColumnName = "cust_loan_amount"], [SourceColumnName = "cust_model", DestinationColumnName = "cust_model"], [SourceColumnName = "cust_teman_seva_name", DestinationColumnName = "cust_teman_seva_name"], [SourceColumnName = "cust_variant", DestinationColumnName = "cust_variant"], [SourceColumnName = "cust_vehicle_model_year", DestinationColumnName = "cust_vehicle_model_year"], [SourceColumnName = "leads_name", DestinationColumnName = "leads_name"], [SourceColumnName = "leads_origination", DestinationColumnName = "leads_origination"], [SourceColumnName = "platform", DestinationColumnName = "platform"], [SourceColumnName = "promo_origination", DestinationColumnName = "promo_origination"], [SourceColumnName = "teman_seva_code", DestinationColumnName = "teman_seva_code"], [SourceColumnName = "teman_seva_trx_code_registration", DestinationColumnName = "teman_seva_trx_code_registration"], [SourceColumnName = "injected_time", DestinationColumnName = "injected_time"], [SourceColumnName = "utm_group", DestinationColumnName = "utm_group"], [SourceColumnName = "channel_group", DestinationColumnName = "channel_group"], [SourceColumnName = "cust_tenor_month", DestinationColumnName = "cust_tenor_month"], [SourceColumnName = "phone_number_first_four_digit", DestinationColumnName = "phone_number_first_four_digit"], [SourceColumnName = "phone_number_length", DestinationColumnName = "phone_number_length"], [SourceColumnName = "qualified_status", DestinationColumnName = "qualified_status"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared poc_data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "f0bae9f7-35d7-42ad-b24d-31286c19623c"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "7c807647-f014-431d-aeb1-c995d23c4826"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "poc_data", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
shared poc_data_DataDestination = let
  Pattern = Fabric.Warehouse([CreateNavigationProperties = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "git-wh"]}[Data],
  TableNavigation = Navigation_2{[Item = "poc_data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
