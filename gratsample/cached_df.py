import os
import pandas as pd

CACHE_ROOT = os.getenv("CACHE_DIR", 'cache')

def make_cached_df(cache_sub_dir):
    """a decorator who input is a direcotry under env's CACHE_DIR
    wraps a DataFrame returning function, and uses the repr of the arguments as a keys"""
    # decorator factory
    def cached_df(df_returning_fn):
        # decorator
        def get_with_cache(*args, **kwargs):
            # wrapping function
            cache_dir = os.path.join(CACHE_ROOT, cache_sub_dir)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir, exist_ok=True)
            all_args = args+tuple(kwargs.values())
            str_args = "_".join([a.__name__ if callable(a) else str(a) for a in all_args])
            str_args = str_args.replace('/', '___')
            fname = f'{str_args}'
            fname = fname[:255] # these can get long
            cache_key = os.path.join(cache_dir, fname)
            try:
                return pd.read_pickle(cache_key)
            except FileNotFoundError:
                df = df_returning_fn(*args, **kwargs)
                df.to_pickle(cache_key)
                return df

        return get_with_cache

    return cached_df
