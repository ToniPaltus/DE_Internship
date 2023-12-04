import argparse


def pasre_args(args: list) -> dict:
    """This is a function which parse command line arguments.
    input: command line arguments
    return: argument's dictionary.

    It has 3 named arguments:
    -students
    -rooms
    -store_format

    Example: python main.py -students='students_file_path' -rooms='rooms_file_path' -store_format='JsonOrXml'
    """
    arguments = {}
    parser = argparse.ArgumentParser(description="It's a command parser for arguments")

    # adding arguments
    parser.add_argument(
        "-students",
        default="",
        action="store",
        dest="students_path",
        type=str,
        required=True,
        help="Add the path of your students file.",
    )
    parser.add_argument(
        "-rooms",
        default="",
        action="store",
        dest="rooms_path",
        type=str,
        required=True,
        help="Add the path of your rooms file.",
    )
    parser.add_argument(
        "-store_format",
        default="",
        action="store",
        dest="store_format",
        type=str,
        help="Choose store file format JSON or XML.",
    )

    # args
    args = parser.parse_args()

    # result dictionary
    words = ["students_path", "rooms_path", "store_format"]
    for word in words:
        work = "args" + "." + word
        arguments.update({word: eval(work)})

    return arguments
