import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

PG_CONNECTION_STRING = "postgresql+psycopg2://catedra:S3cret@postgres/catedra"

context = gx.get_context(project_root_dir="/gq/gx")


try:
    pg_datasource = context.sources.add_postgres(
        name="pg_datasource", connection_string=PG_CONNECTION_STRING
    )
except gx.exceptions.exceptions.DataContextError:
    print("Datasource already exists, instantiating..")
    pg_datasource = context.datasources["pg_datasource"]
    
try:
    pg_datasource.add_table_asset(
        name="turnstiles", table_name="turnstiles"
    )
except ValueError:
    # Already exists
    pass

batch_request = pg_datasource.get_asset("turnstiles").build_batch_request()

expectation_suite_name = "suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


##########################
# validator.expect_x(...)#
##########################

checkpoint = Checkpoint(
    name="turnstiles_checkpoint",
    run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
    data_context=context,
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
    action_list=[
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ],
)


context.add_or_update_checkpoint(checkpoint=checkpoint)
validator.save_expectation_suite()


checkpoint_result = checkpoint.run()

context.open_data_docs()
