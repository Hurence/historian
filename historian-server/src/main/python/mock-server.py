from flask import Flask, json, request

labels = {"status": "success",
          "data": ["U004_TC01", "U004_TC02"]}

series = {"status": "success",
         "data": [
             {"__name__": "U004_TC01", "metric": "UT800D.U004_TC01_OP.F_CV", "type": "temperature", "sub_unit": "reacteur1_coquille1", "measure": "pour_cent_op"},
             {"__name__": "U004_TC01", "metric": "UT800D.U004_TC01_PV.F_CV", "type": "temperature", "sub_unit": "reacteur1_coquille1", "measure": "mesure_pv"},
             {"__name__": "U004_TC01", "metric": "UT800D.U004_TC01_SP.F_CV", "type": "temperature", "sub_unit": "reacteur1_coquille1", "measure": "consigne_sp"},
             {"__name__": "U004_TC02", "metric": "UT800D.U004_TC02_OP.F_CV", "type": "temperature", "sub_unit": "reacteur1_coquille2", "measure": "pour_cent_op"},
             {"__name__": "U004_TC02", "metric": "UT800D.U004_TC02_PV.F_CV", "type": "temperature", "sub_unit": "reacteur1_coquille2", "measure": "mesure_pv"},
         ]}

metadata = { "status": "success",
             "data": {
                "U204": [
                    {
                        "type": "gauge",
                        "help": "Unit 204",
                        "unit": ""
                    }
                ],
                "U879-E2": [
                    {
                        "type": "gauge",
                        "help": "Unit U879-E2",
                        "unit": ""
                    }
                ]
            }
             }

query_response = {"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"go_gc_duration_seconds","instance":"prometheus:9090","job":"services","quantile":"0"},"value":[1616429553,"0.0000798"]},{"metric":{"__name__":"go_gc_duration_seconds","instance":"prometheus:9090","job":"services","quantile":"0.25"},"value":[1616429553,"0.0001747"]},{"metric":{"__name__":"go_gc_duration_seconds","instance":"prometheus:9090","job":"services","quantile":"0.5"},"value":[1616429553,"0.0002521"]},{"metric":{"__name__":"go_gc_duration_seconds","instance":"prometheus:9090","job":"services","quantile":"0.75"},"value":[1616429553,"0.0004114"]},{"metric":{"__name__":"go_gc_duration_seconds","instance":"prometheus:9090","job":"services","quantile":"1"},"value":[1616429553,"0.0048405"]}]}}

query_range_response = {"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"U004","type": "temperature", "sub_unit": "reacteur1_coquille1", "measure": "pour_cent_op"},"values":[[1616425950,"0.0000535"],[1616425965,"0.0000545"],[1616425980,"0.0000555"]]},{"metric":{"__name__": "U004", "type": "temperature", "sub_unit": "reacteur1_coquille1", "measure": "mesure_pv"},"values":[[1616425950,"0.000153"],[1616425965,"0.000123"],[1616425980,"0.000153"]]}]}}

rules = {"status": "success", "data": ""}

api = Flask(__name__)


@api.route('/api/v1/rules', methods=['GET'])
def get_rules():
    return json.dumps(rules)


@api.route('/api/v1/query', methods=['POST'])
def get_query():
    ''' save and test in datasource setup : ([('query', '1+1'), ('time', '1616404376.622')] '''
    # Called when shift-Enter and print data in table view (only one point for each metric)

    time = request.args.get('time', '')
    query = request.args.get('query', '') # 'query', 'U204{measure="consigne_sp" sub_unit="reacteur1_coquille1" type="temperature"}'

    print('/api/v1/query', request.args)
    return json.dumps(query_response)


@api.route('/api/v1/query_range', methods=['GET'])
def get_query_range():
    # Called when shift-Enter and print data

    start = request.args.get('start', '')
    end = request.args.get('end', '')
    step = request.args.get('step', '')
    query = request.args.get('query', '') # 'query', 'U204{measure="consigne_sp" sub_unit="reacteur1_coquille1" type="temperature"}'

    print('/api/v1/query_range', request.args)
    return json.dumps(query_range_response)


@api.route('/api/v1/label/__name__/values', methods=['GET'])
def get_label_values():
    # get
    #print('/api/v1/label/__name__/values', request.args)

    start = request.args.get('start', '')
    end = request.args.get('end', '')
    return json.dumps(labels)


@api.route('/api/v1/metadata', methods=['GET'])
def get_metadata():
    print('/api/v1/metadata', request.args)
    return json.dumps(metadata)


@api.route('/api/v1/series', methods=['GET'])
def get_series():
    # Called after each change in metrics search box => update series value
    match_args_str = request.args.get('match[]', '').replace('{', '').replace('}', '')
    match_args = {}
    for i in match_args_str.split(','):
        tokens = i.split('=')
        match_args[tokens[0]] = tokens[1]

    print(match_args)
    start = request.args.get('start', '')
    end = request.args.get('end', '')

    return json.dumps(series)


if __name__ == '__main__':
    api.run(host="0.0.0.0", port=8081)
