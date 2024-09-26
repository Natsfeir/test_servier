import re
class Ingestion:
    """Abstract class for ingestion tasks."""
    def load(self, bucket_name: str, blob_path: str):
        raise NotImplementedError
    
    def clean(self, file: str):
        pass
    
    def run(self, table_name: str):
        raise NotImplementedError
    @classmethod
    def clean_json_string(cls, json_str):
        # Remove trailing commas that are before closing brackets
        cleaned_str = re.sub(r',\s*(\}|\])', r'\1', json_str)
        return cleaned_str
