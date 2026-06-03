from .bronze_generator import BronzeGenerator
from .silver_generator import SilverGenerator

__all__ = ["BronzeGenerator", "SilverGenerator"]


def generate_all_models(source_name: str) -> None:
    bronze_gen = BronzeGenerator()
    bronze_gen.generate_source(source_name)

    silver_gen = SilverGenerator()
    silver_gen.generate_source(source_name)
