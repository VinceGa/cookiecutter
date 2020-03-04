import os
import tempfile
from contextlib import contextmanager
from typing import ContextManager, Union



@contextmanager
def atomic_write(
    file: Union[str, os.PathLike], mode: str = "w", as_file: bool = True, **kwargs
) -> ContextManager:
    """Write a file atomically

    :param file: str or :class:`os.PathLike` target to write
    :param mode: the mode in which the file is opened, defaults to "w" (writing in text mode)
    :param bool as_file:  if True, the yielded object is a :class:File.
        (eg, what you get with `open(...)`).  Otherwise, it will be the
        temporary file path string

    :param kwargs: anything else needed to open the file

    :raises: FileExistsError if target exists

    Example::

        with atomic_write("hello.txt") as f:
            f.write("world!")

    """
    if os.path.exists(file):
        raise FileExistsError("The file {} already exists.".format(file))

    # retrieve file extension from path
    file_extension = os.path.splitext(file)[-1]

    fail_flag = False  

    # generate temporary file with random filename in the same directory
    # note the delete param is set to False as the default.
    with tempfile.NamedTemporaryFile(
        mode=mode,
        suffix=file_extension,
        dir=os.path.dirname(file),
        delete=False,
        **kwargs,
    ) as tmp_file:
        try:
            # if as_file flag is True, yield the temporary file
            if as_file:
                yield tmp_file
            else:  # otherwise return the temporary file path as string
                yield tmp_file.name
        except Exception as e:
            # catch error, set failure flag to True, and re-throw error
            fail_flag = True
            raise e
        finally:
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
            tmp_file.close()
            # if failure occurred, then remove the incomplete file
            if fail_flag:
                # remove file
                if os.path.exists(file): #pragma: no cover
                    os.remove(file)
            else:
                # otherwise rename temp file to the target name
                os.rename(tmp_file.name, file)

            # make sure the temporary file is removed
            if os.path.exists(tmp_file.name):
                os.remove(tmp_file.name)

