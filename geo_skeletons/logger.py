import logging

class Config:
    verbose = False


def set_verbose(value: bool):
    """
    Enable or disable verbose logging globally for the application.

    This function sets the global verbosity level by modifying the `Config.verbose` flag 
    and adjusting the logging behavior accordingly. When `value` is `True`, the logger 
    is set to `INFO` level to display informational messages and above. When `value` is 
    `False`, the logger is silenced by setting its level to `CRITICAL + 1`, effectively 
    disabling all logging output.

    Args:
        value (bool): 
            - True to enable verbose logging (INFO level and above).
            - False to disable all logging output.
    """
    Config.verbose = value
    if value:
        log.setLevel(logging.INFO)  # Enable INFO and higher messages
    else:
        log.setLevel(logging.CRITICAL + 1)  # Disable all logging


def is_verbose(verbose: bool = None) -> bool:
    """
    Determine if verbose mode is enabled.
    
    If `verbose` is provided (not None), use its value.
    Otherwise, fall back to the global `Config.verbose` setting.

    Args:
        verbose (bool, optional): Local verbose setting. Defaults to None.

    Returns:
        bool: True if verbose mode is enabled, False otherwise.
    """
    return verbose if verbose is not None else Config.verbose

log = logging.getLogger("geo-skeletons")
log.setLevel(logging.INFO)
handler = logging.StreamHandler() 
handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
log.addHandler(handler)
