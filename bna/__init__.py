def init_logging():
    import logging
    from logging.handlers import SocketHandler
    log = logging.getLogger('BNA')
    socket_handler = SocketHandler('127.0.0.1', 19996)
    term_handler = logging.StreamHandler()

    try:
        import colorlog
        fmt = colorlog.ColoredFormatter('%(asctime)s %(log_color)s[%(name)6s:%(lineno)3s'
                                        ']\t%(levelname)-.5s  %(message)s', datefmt="%H:%M:%S")
    except ImportError:
        fmt = logging.Formatter('%(asctime)s [%(name)12s:%(lineno)3s'
                                ']\t%(levelname)-.6s  %(message)s',
                                datefmt="%H:%M:%S")

    term_handler.setFormatter(fmt)
    log.addHandler(term_handler)
    log.addHandler(socket_handler)
    log.setLevel(1)
    return log
