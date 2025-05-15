import great_expectations as gx
from great_expectations.checkpoint import Checkpoint

#El connection string linkea con nuestra BBDD. El context es el marco en el cual desarrollaremos el trabajo

PG_CONNECTION_STRING = "postgresql+psycopg2://catedra:S3cret@postgres/catedra"

context = gx.get_context(project_root_dir="/gq")


#Intentamos agregar al contexto una fuente de datos (una BBDD en postgres donde previamente creamos
#y populamos la tabla turnstiles. En caso de que corramos por 2da vez, simplemente accedemos a esta fuente
#que ya fue creada antes

try:
    pg_datasource = context.sources.add_postgres(
        name="pg_datasource", connection_string=PG_CONNECTION_STRING
    )
except gx.exceptions.exceptions.DataContextError:
    del context.datasources["pg_datasource"]
    pg_datasource = context.sources.add_postgres(
        name="pg_datasource", connection_string=PG_CONNECTION_STRING
    )

#Misma lógica pero para agregar la tabla, si ya existe simplemente es pass
try:
    pg_datasource.add_table_asset(name="turnstiles", table_name="turnstiles")
except ValueError:
    # Already exists
    pass


#Intentamos agregar al contexto una fuente de datos (una BBDD en postgres donde previamente creamos
#y populamos la tabla turnstiles. En caso de que corramos por 2da vez, borramos la fuente y la volvemos a crear. Para que la tabla no quede "cacheada"

batch_request = pg_datasource.get_asset("turnstiles").build_batch_request()



expectation_suite_name = "suite"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)


#Aca debieran escribir las expectations deseadas, estas se agregarán al suite al ser corridas por el validator
#El punto uno estáde ejemplo, ayúdense con la docu
##########################
#2
#4
validator.expect_column_values_to_not_be_null("molinete")
##########################

#Un checkpoint es una abstracción resultante de validar una suite de expectations 
#Le estamos pidiendo además que actualice data docs (html) y guarde el resultado de la validación
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

#En estas útimas líneas construimos el .html con los docs, el documento suite.json y corremos el checkpoint
context.build_data_docs()
context.add_or_update_checkpoint(checkpoint=checkpoint)
validator.save_expectation_suite(discard_failed_expectations=False)

# Observación: cuando ya se tiene un checkpoint pre-configurado, se puede usar:
# retrieved_checkpoint = context.get_checkpoint(name="my_checkpoint")
checkpoint_result = checkpoint.run()

context.open_data_docs()
