import pandas as pd
from utils import count_html_tags
from utils import count_non_utf8_chars

def general_validations(df:pd.DataFrame):
    """
    Some general df validations that will run on a dataframe
    """
    stats = {}
    nullsintxt = int(df['text'].isna().sum())
    stats['Nulls in text data'] = nullsintxt

    # html checking
    df["element_count"] = df["text"].apply(count_html_tags)
    stats['Html tags'] = int(df['element_count'].sum())

    # utf8 encoding checking
    df['non-utf8_count'] = df["text"].apply(count_non_utf8_chars)
    stats['Utf8 chars'] = int(df['non-utf8_count'].sum())

    return stats

def deduplication_validation(df:pd.DataFrame):
    """
    Input df and check for issues created in deduplication
    """
    stats = {}
    exactduplicates = int(df['text'].duplicated().sum())
    stats['Duplicates in text'] = exactduplicates
    return stats

class Validator:
    """
    Class to validate df's once they go through a pipeline step
    """
    def __init__(self):
        self.stats = {}

    def validate(self, df: pd.DataFrame):
        """
        Validate the df procudced in the Pipelinestep. Overwritten for other validation types
        """
        raise NotImplementedError

class GeneralValidator(Validator):
    """
    The purpose of this class is to run a bunch of general validation steps
    """
    def __init__(self):
        super().__init__()
    
    def validate(self, df: pd.DataFrame):
        """
        Run general validations
        """
        self.stats = general_validations(df) # always start self.stats with general stats

class DeduplicationValidator(Validator):
    """
    The purpose of this class is to validate the duplication steps are performed correctly
    """
    def __init__(self):
        super().__init__()

    def validate(self, df: pd.DataFrame):
        """
        Run deduplication validation along with general
        """
        self.stats = general_validations(df)
        validation_stats = deduplication_validation(df)
        self.stats.update(validation_stats)