"""Parsing Heroku app datanator-rest-api
"""
import re


class ParseLogs:

    def __init__(self, file_location=None, verbose=True):
        """Init

        Args:
            file_location(:obj:`str`): location of log file.
            verbose(:obj:`bool`): print verbose message.
        """
        self.file_location = file_location
        self.verbose = verbose

    def parse_regex(self, s, regex):
        """Find in line matches for regulation expressions.

        Args:
            s(:obj:`str`): String in which to find matching regular expression.
            regex(:obj:re`): regular expression.

        Return:
            (:obj:`list`): Matches found. 
        """
        return re.search(regex, s)

    def parse_router(self, lines=0):
        """Parse router lines to get API performance values.

        Args:
            lines(:obj:`int`, optional): Number of lines to parse. If 0, parse all lines.

        Return:
            (:obj:`Obj`)
        """
        result = {}
        with open(self.file_location, 'r') as _file:
            for i, line in enumerate(_file):
                if lines != 0 and i == lines:
                    break
                if line.endswith('\n'):
                    line = line[0:-1]
                end_point = re.search(r'(\/.*\w+\/)', line).group(1)
                performance = int(re.search(r'service=(\d*)ms', line).group(1))
                if result.get(end_point) is None:
                    result[end_point] = [performance]
                else:
                    result[end_point] += [performance]
        return result

                