

class TaggableType():

    type_tag = False

    @classmethod
    def by_type_tag(cls, tag):
        for sub in cls.__subclasses__():
            if sub.type_tag == tag:
                return sub
