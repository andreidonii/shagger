import jsonschema


class Validate():
    def __init__(self, config: dict):
        self.__config = config

    def check_in(self, payload: dict, topic: str):
        return self.check(payload, topic, True)

    def check_out(self, payload: dict, topic: str):
        return self.check(payload, topic, False)

    def check(self, payload: dict, topic: str, incoming: bool):
        pass
        # test = payload
        # if 'args' in payload and 'kwargs' in payload:
        #     test = payload['args'][0]
        # print('HERE', test, self.__config['in' if incoming else 'out'][topic])
        # jsonschema.validate(test, self.__config['in' if incoming else 'out'][topic])