from .bronze_generator import generate_bronze_models
from .silver_generator import generate_silver_model

__all__ = ["generate_bronze_models", "generate_silver_model"]


def generate_all_models(source_name: str) -> None:
    generate_bronze_models(source_name)
    tables = __import__("platform.registry", fromlist=["get_tables_for_source"]).get_tables_for_source(source_name)
    for table in tables:
        generate_silver_model(source_name, table["table_name"])
