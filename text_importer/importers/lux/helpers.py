def convert_coordinates(hpos, vpos, width, height, x_res, y_res):
    """
    x =   (coordinate['xResolution']/254.0) * coordinate['hpos']

    y =   (coordinate['yResolution']/254.0) * coordinate['vpos']

    w =  (coordinate['xResolution']/254.0) * coordinate['width']

    h =  (coordinate['yResolution']/254.0) * coordinate['height']
    """
    x = (x_res / 254) * hpos
    y = (y_res / 254) * vpos
    w = (x_res / 254) * width
    h = (y_res / 254) * height
    return [int(x), int(y), int(w), int(h)]


def encode_ark(ark):
    """Replaces (encodes) backslashes in the Ark identifier."""
    return ark.replace('/', '%2f')
