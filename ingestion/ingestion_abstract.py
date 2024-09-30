import re


class Ingestion:
    """Abstract class for ingestion tasks."""

    @classmethod
    def clean_json_string(cls, json_str):
        # Remove trailing commas that are before closing brackets
        cleaned_str = re.sub(r",\s*(\}|\])", r"\1", json_str)
        return cleaned_str
