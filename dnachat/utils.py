def to_comparable(version):
    return tuple(int(x) for x in version.split('.'))
