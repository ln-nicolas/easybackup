

class Hook():

    registery = {}

    @classmethod
    def register(cls, hook_name):
        def decorator(func):

            hooks = cls.registery.get(hook_name, [])
            hooks.append(func)
            cls.registery[hook_name] = hooks

            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
            return wrapper
        return decorator

    @classmethod
    def plays(cls, hook_name, *args, **kwargs):
        for hook in cls.registery.get(hook_name, []):
            hook(*args, **kwargs)
