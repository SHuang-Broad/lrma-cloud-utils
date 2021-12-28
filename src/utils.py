import logging
import logging.handlers
import sys


def get_configured_logger(log_level: int = logging.DEBUG,
                          flush_level: int = logging.ERROR,
                          buffer_capacity: int = 0,
                          formatter: logging.Formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')):

    handler = __config_stream_or_file_handler(log_level, formatter)
    memory_handler = __config_memory_handler(buffer_capacity, flush_level, handler)

    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(memory_handler)
    return logger


def __config_stream_or_file_handler(log_level: int, formatter: logging.Formatter, log_file: str = None):
    if log_file:
        handler = logging.FileHandler(log_file)
    else:
        handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(log_level)
    handler.setFormatter(formatter)
    return handler


def __config_memory_handler(capacity: int, flush_level: int, handler: logging.handlers):
    return logging.handlers.MemoryHandler(
        capacity=capacity,
        flushLevel=flush_level,
        target=handler
    )


def get_dict_depth(d: dict, level: int) -> int:
    """
    Simple utility (using recursion) to get the max depth of a dictionary
    :param d: dict to explore
    :param level: current level
    :return: max depth of the given dict
    """
    cls = [level]
    for _, v in d.items():
        if isinstance(v, dict):
            cls.append(get_dict_depth(v, 1 + level))

    return max(cls)


def is_contiguous(arr: list) -> bool:
    arr_shift = arr[1:]
    arr_shift.append(arr[-1]+1)
    return all(1 == (a_i - b_i) for a_i, b_i in zip(arr_shift, arr))