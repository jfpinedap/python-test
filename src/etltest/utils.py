"""Utils."""

# Base imports
import time


def timeit(method):
    """Function method resolve data based into resolve"""
    def timed(*args, **kw):
        """Handle timed resolve"""
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('Time to solve %r: %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result
    return timed
