import pandas as pd
import re
from utils import general_validations


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
