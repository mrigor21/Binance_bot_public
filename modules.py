# Декораторы для отказоустойчивости алгоритма в случае, если API не отвечает.
# Друг от друга мало чем отличаются, хотя в первых версиях различались сильно.
# Сейчас можно написать один универсальный декоратор, закрывающий все случаи.

from loguru import logger
import time


def try_request(expected_response=200, try_amount=5, pause=10, service='Сервис'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    if ans.status_code == expected_response:
                        break
                    else:
                        logger.error(f'Response: {ans.text}')
                        raise Exception(f"Код полученного ответа не соответствует ожидаемому ({expected_response})")
                except Exception:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.exception(f'Сервис "{service}" не отвечает, повторная попытка подключения через {pause} секунд')
                        time.sleep(pause)
                    else:
                        logger.exception(f'Сервис "{service}" не ответил за предельное количество попыток')
                        # raise Exception(f'Сервис "{service}" не ответил за предельное количество попыток')
            return ans
        return wrapper
    return decorator


def try_get_server_time(try_amount=3, pause=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    logger.success(f'Ордер отправлен {args} {kwargs}')
                    break
                except Exception as e:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Не удалось получить время сервера, повторная попытка через {pause} секунд, повторная попытка через {pause} секунд:\n{e}')
                        time.sleep(pause)
                    else:
                        logger.error(f'Не удалось получить время сервера за предельное количество попыток:\n{e}')
                        # raise Exception
            if 'ans' in locals():
                return ans
        return wrapper
    return decorator


def try_submit_order(raise_exception=True, try_amount=3, pause=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    logger.success(f'Ордер отправлен {args} {kwargs}')
                    break
                except Exception as e:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Не удалось отправить ордер {args} {kwargs}, повторная попытка:\n{e}')
                        time.sleep(pause)
                    else:
                        logger.error(f'Не удалось отправить ордер {args} {kwargs} за предельное количество попыток:\n{e}')
                        if raise_exception:
                            raise Exception
            if 'ans' in locals():
                return ans
        return wrapper
    return decorator


def try_get_orders(try_amount=3, pause=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    logger.success(f'Список ордеров получен {args} {kwargs}')
                    break
                except Exception as e:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Не удалось получить список ордеров {args} {kwargs}, повторная попытка через {pause} секунд:\n{e}')
                        time.sleep(pause)
                    else:
                        logger.exception(f'Не удалось получить список ордеров {args} {kwargs} за предельное количество попыток\n{e}')
                        raise Exception
            return ans
        return wrapper
    return decorator


def try_cancel_order(try_amount=3, pause=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    logger.success(f'Ордер отменен {args} {kwargs}')
                    break
                except Exception as e:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Не удалось отменить ордер {args} {kwargs}, повторная попытка через {pause} секунд:\n{e}')
                        time.sleep(pause)
                    else:
                        logger.exception(f'Не удалось отменить ордер {args} {kwargs} за предельное количество попыток\n{e}')
                        raise Exception
            return ans
        return wrapper
    return decorator


def try_close_position(try_amount=3, pause=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for try_num in range(try_amount):
                try:
                    ans = func(*args, **kwargs)
                    logger.success(f'Позиция закрыта {args} {kwargs}')
                    break
                except Exception as e:
                    if try_num < try_amount - 1:
                        # error_message = traceback.format_exc()
                        logger.error(f'Не удалось закрыть позицию {args} {kwargs}, повторная попытка через {pause} секунд:\n{e}')
                        time.sleep(pause)
                    else:
                        logger.exception(f'Не удалось закрыть позицию {args} {kwargs} за предельное количество попыток\n{e}')
                        raise Exception
            return ans
        return wrapper
    return decorator






