from __future__ import annotations

import base64
import logging
import json
import pandas as pd
import pandas_gbq
from datetime import datetime
import google.cloud.dialogflowcx_v3beta1.types as dfcx_types
from dfcx_scrapi.core.agents import Agents
from dfcx_scrapi.core.intents import Intents
from dfcx_scrapi.core.entity_types import EntityTypes
from dfcx_scrapi.core.flows import Flows
from dfcx_scrapi.core.pages import Pages
from dfcx_scrapi.core.webhooks import Webhooks
from dfcx_scrapi.core.transition_route_groups import TransitionRouteGroups
from proto.marshal.collections.maps import MapComposite
from proto.marshal.collections.repeated import RepeatedComposite

# Type aliases
DFCXFlow = dfcx_types.flow.Flow
DFCXPage = dfcx_types.page.Page
DFCXRoute = dfcx_types.page.TransitionRoute
DFCXCase = dfcx_types.Fulfillment.ConditionalCases.Case

# logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Get current date
curr_date = str(datetime.now())


def get_all_flow_data(dfcx_flows, agent_id):
    flow_data = {}
    flow_list = dfcx_flows.list_flows(agent_id)
    for flow in flow_list:
        flow_data[flow.name] = flow
    return flow_data


def get_all_page_data(flows_map, dfcx_pages):
    page_data = {}
    for flow_id in flows_map.keys():
        page_list = dfcx_pages.list_pages(flow_id=flow_id)
        page_data[flow_id] = {page.name: page for page in page_list}
    return page_data


def get_all_route_group_data(flows_map, dfcx_route_groups):
    route_group_data = {}
    for flow_id in flows_map.keys():
        group_list = dfcx_route_groups.list_transition_route_groups(
            flow_id=flow_id
        )
        route_group_data[flow_id] = {rg.name: rg for rg in group_list}
    return route_group_data


def parse_value(value):
    return str(value)  # Placeholder


def map_page_name(flow_id, page_id, page_dict):
    if flow_id in page_dict and page_id in page_dict[flow_id]:
        return page_dict[flow_id][page_id]
    if page_id is not None:
        return 'Start'  # Special case
    return None


def convert_protobuf(obj):
    """Recursive function to convert protobuf object to
    python object with lists and/or dictionaries"""

    if isinstance(obj, MapComposite):
        res = {}
        for key, value in obj.items():
            res[key] = convert_protobuf(value)
        return res
    elif isinstance(obj, RepeatedComposite):
        res = []
        for value in obj:
            res.append(convert_protobuf(value))
        return res
    else:
        return obj


def parse_fulfillment(fulfillment, webhook_dict):
    messages = []
    # Parse the different fulfillment message types
    if getattr(fulfillment, 'messages', None):
        for message in fulfillment.messages:
            if getattr(message, 'text', None):
                message_options = list(message.text.text)  # list
                messages.append(json.dumps(
                    {'type': 'Agent says', 'data': message_options}))
            if getattr(message, 'payload', None):
                message_payload = convert_protobuf(message.payload)  # dict
                messages.append(json.dumps(
                    {'type': 'Custom payload', 'data': message_payload}))
            if getattr(message, 'live_agent_handoff', None):
                message_metadata = convert_protobuf(
                    message.live_agent_handoff.metadata)  # dict
                messages.append(json.dumps(
                    {'type': 'Live agent handoff', 'data': message_metadata}))
            if getattr(message, 'conversation_success', None):
                message_metadata = convert_protobuf(
                    message.conversation_success.metadata)  # dict
                messages.append(json.dumps(
                    {'type': 'Conversation success metadata', 'data': message_metadata}))
            if getattr(message, 'output_audio_text', None):
                message_ssml = message.output_audio_text.ssml  # str
                messages.append(json.dumps(
                    {'type': 'Output audio text', 'data': message_ssml}))
            # Other unused options: Play pre-recorded audio, Telephony transfer call

    # Conditional response needs to be handled differently
    if getattr(fulfillment, 'conditional_cases', None):
        for conditional_response in fulfillment.conditional_cases:
            cond_res_text = parse_conditional_fulfillment(conditional_response)
            messages.append(json.dumps(
                {'type': 'Conditional response', 'data': cond_res_text}))

    webhookId = getattr(fulfillment, 'webhook', None)
    webhookName = webhook_dict[getattr(fulfillment, 'webhook', None)] if getattr(
        fulfillment, 'webhook', None) in webhook_dict else None
    webhookTag = getattr(fulfillment, 'tag', None)
    partialResponse = getattr(fulfillment, 'return_partial_responses', False)
    parameterPresets = []  # {}
    if getattr(fulfillment, 'set_parameter_actions', None):
        for param_preset in fulfillment.set_parameter_actions:
            # List form
            parameterPresets.append(json.dumps(
                {"parameter": param_preset.parameter, "value": parse_value(param_preset.value)}))
            # Dict form
            # parameterPresets[param_preset.parameter] = parse_value(param_preset.value)
    return {
        'messages': messages,
        'webhookId': webhookId,
        'webhookName': webhookName,
        'webhookTag': webhookTag,
        'partialResponse': partialResponse,
        'parameterPresets': parameterPresets
    }


def parse_conditional_fulfillment(conditional_response, cond_res_text='', tabs=0):
    tab_symbol = '  '
    if isinstance(conditional_response, DFCXCase) and tabs == 0:
        cond_res_text += tab_symbol*tabs + 'if '
        if getattr(conditional_response, 'condition', None):
            cond_res_text += conditional_response.condition + '\n'
    else:
        for i, case in enumerate(conditional_response.cases):
            if i == 0:
                cond_res_text += tab_symbol*tabs + 'if '
            elif i != len(conditional_response.cases) - 1:
                cond_res_text += tab_symbol*tabs + 'elif '
            else:
                cond_res_text += tab_symbol*tabs + 'else\n'
            if getattr(case, 'condition', None):
                cond_res_text += case.condition + '\n'
            for content in case.case_content:
                tabs += 1
                if getattr(content, 'additional_cases', None):
                    parse_conditional_fulfillment(
                        content.additional_cases, cond_res_text, tabs)
                    # Nested conditions, use recursion (note cond_res_text side effect)
                    # for additional_case in content.additional_cases.cases:
                    #     parse_conditional_fulfillment_new(
                    #         additional_case, cond_res_text, tabs)
                elif getattr(content, 'message', None):
                    if getattr(content.message, 'text', None):
                        # For some reason this is a list even though it can only be one thing...
                        text_message = content.message.text.text[0]
                        cond_res_text += tab_symbol*tabs + text_message + '\n'
                tabs -= 1
    return cond_res_text


# def parse_conditional_fulfillment(conditional_response, cond_res_text='', tabs=0):
#     tab_symbol = '  '
#     for i, case in enumerate(conditional_response.cases):
#         if i == 0:
#             cond_res_text += tab_symbol*tabs + 'if '
#         elif i != len(conditional_response.cases) - 1:
#             cond_res_text += tab_symbol*tabs + 'elif '
#         else:
#             cond_res_text += tab_symbol*tabs + 'else\n'
#         if getattr(case, 'condition', None):
#             cond_res_text += case.condition + '\n'
#         for content in case.case_content:
#             tabs += 1
#             if getattr(content, 'additional_cases', None):
#                 # Nested conditions, use recursion (note cond_res_text side effect)
#                 for additional_case in content.additional_cases.cases:
#                     parse_conditional_fulfillment(
#                         additional_case, cond_res_text, tabs)
#             elif getattr(content, 'message', None):
#                 if getattr(content.message, 'text', None):
#                     # For some reason this is a list even though it can only be one thing...
#                     text_message = content.message.text.text[0]
#                     cond_res_text += tab_symbol*tabs + text_message + '\n'
#             tabs -= 1
#     return cond_res_text


def parse_routes(route_list, agent_id, agent_name, flow_id, flow_dict, page_dict, route_group_dict, webhook_dict, intent_dict, page_id=None, route_group_id=None, parameter_id=None, parameter_name=None):
    routes = []
    routes.append(pd.DataFrame(columns=['date', 'agentId', 'agentName', 'flowId', 'flowName', 'pageId', 'pageName', 'transitionRouteId', 'routeGroupId', 'routeGroupName', 'intentId', 'intentName',
                  'parameterId', 'parameterName', 'targetPageId', 'targetPageName', 'webhookId', 'webhookName', 'webhookTag', 'event', 'condition',  'fulfillment',  'partialResponse', 'parameterPresets']))
    for route in route_list:
        fulfillment = parse_fulfillment(
            route.trigger_fulfillment, webhook_dict)
        # Parse target
        target_page_id = None
        target_page_name = None
        if getattr(route, 'target_flow', None):
            target_page_id = getattr(route, 'target_flow')
            target_page_name = flow_dict[target_page_id]
        elif getattr(route, 'target_page', None):
            # Special cases?
            target_page_id = getattr(route, 'target_page', None)
            if flow_id in page_dict and target_page_id in page_dict[flow_id]:
                target_page_name = page_dict[flow_id][target_page_id]
            else:
                target_page_name = target_page_id.split("/")[-1]
        route_df = pd.DataFrame({
            'date': [curr_date],
            'agentId': [agent_id],
            'agentName': [agent_name],
            'flowId': [flow_id],
            'flowName': [flow_dict[flow_id]],
            'pageId': [page_id],
            'pageName': [map_page_name(flow_id, page_id, page_dict)],
            'transitionRouteId': [route.name],
            'routeGroupId': [route_group_id],
            'routeGroupName': [route_group_dict[flow_id][route_group_id] if flow_id in route_group_dict and route_group_id in route_group_dict[flow_id] else None],
            'intentId': [getattr(route, 'intent', None)],
            'intentName': [intent_dict[getattr(route, 'intent', None)] if getattr(route, 'intent', None) in intent_dict else None],
            'parameterId': [parameter_id],
            'parameterName': [parameter_name],
            'targetPageId': [target_page_id],
            'targetPageName': [target_page_name],
            'webhookId': [fulfillment['webhookId']],
            'webhookName': [fulfillment['webhookName']],
            'webhookTag': [fulfillment['webhookTag']],
            'event': [getattr(route, 'event', None)],
            'condition': [getattr(route, 'condition', None)],
            'fulfillment': [fulfillment['messages']],
            'partialResponse': [fulfillment['partialResponse']],
            'parameterPresets': [fulfillment['parameterPresets']]
        })
        routes.append(route_df)
    return pd.concat(routes)


def parse_parameters(form, agent_id, agent_name, flow_id, page_id, flow_dict, page_dict, route_group_dict, webhook_dict, intent_dict, entity_dict):
    parameters = []
    parameter_routes = []
    parameters.append(pd.DataFrame(columns=['date', 'agentId', 'agentName', 'flowId', 'flowName', 'pageId', 'pageName', 'parameterId', 'parameterName', 'entityId',
                      'entityName', 'webhookId', 'webhookName', 'webhookTag', 'required', 'isList', 'redactInLog', 'fulfillment', 'partialResponse', 'parameterPresets', 'routes']))
    parameter_routes.append(pd.DataFrame(columns=['date', 'agentId', 'agentName', 'flowId', 'flowName', 'pageId', 'pageName', 'transitionRouteId', 'routeGroupId', 'routeGroupName', 'intentId', 'intentName',
                            'parameterId', 'parameterName', 'targetPageId', 'targetPageName', 'webhookId', 'webhookName', 'webhookTag', 'event', 'condition',  'fulfillment',  'partialResponse', 'parameterPresets']))
    for parameter in form.parameters:
        # Composite since there isn't one in CX
        parameter_id = page_id + '/' + parameter.display_name
        fulfillment = parse_fulfillment(
            parameter.fill_behavior.initial_prompt_fulfillment, webhook_dict)
        event_handlers = parse_routes(parameter.fill_behavior.reprompt_event_handlers, agent_id, agent_name, flow_id, flow_dict, page_dict,
                                      route_group_dict, webhook_dict, intent_dict, page_id=page_id, parameter_id=parameter_id, parameter_name=parameter.display_name)
        parameter_routes.append(event_handlers)
        route_ids = list(event_handlers['transitionRouteId'])
        entity_id = parameter.entity_type
        entity_name = entity_dict[entity_id] if entity_id in entity_dict else entity_id.split(
            "/")[-1]
        parameter_df = pd.DataFrame({
            'date': [curr_date],
            'agentId': [agent_id],
            'agentName': [agent_name],
            'flowId': [flow_id],
            'flowName': [flow_dict[flow_id]],
            'pageId': [page_id],
            'pageName': [map_page_name(flow_id, page_id, page_dict)],
            'parameterId': [parameter_id],
            'parameterName': [parameter.display_name],
            'entityId': [entity_id],
            'entityName': [entity_name],
            'webhookId': [fulfillment['webhookId']],
            'webhookName': [fulfillment['webhookName']],
            'webhookTag': [fulfillment['webhookTag']],
            'required': [getattr(parameter, 'required', False)],
            'isList': [getattr(parameter, 'is_list', False)],
            'redactInLog': [getattr(parameter, 'redact_in_log', False)],
            'fulfillment': [fulfillment['messages']],
            'partialResponse': [fulfillment['partialResponse']],
            'parameterPresets': [fulfillment['parameterPresets']],
            # 'dtmfEnabled': [False], # This actually isn't accessible in scrapi, apparently
            'routes': [route_ids]
        })
        parameters.append(parameter_df)
    return pd.concat(parameters), pd.concat(parameter_routes)


def load_agent_data(agent_id, agent_name):
    print("Initializing Scrapi...")

    # Initialize scrapi
    dfcx_flows = Flows(agent_id=agent_id)
    dfcx_pages = Pages()
    dfcx_route_groups = TransitionRouteGroups(
        agent_id=agent_id
    )
    dfcx_webhooks = Webhooks(agent_id=agent_id)
    dfcx_intents = Intents(agent_id=agent_id)
    dfcx_entities = EntityTypes(agent_id=agent_id)

    print("Generating maps...")

    # Generate maps
    flows_map = dfcx_flows.get_flows_map(agent_id=agent_id)
    # flows_map_rev = dfcx_flows.get_flows_map(
    #    agent_id=agent_id, reverse=True
    # )
    pages_map = {}
    for flow_id in flows_map.keys():
        pages_map[flow_id] = dfcx_pages.get_pages_map(flow_id=flow_id)
    # pages_map_rev = {}
    # for flow_id in flows_map.keys():
    #    pages_map_rev[flow_id] = dfcx_pages.get_pages_map(
    #        flow_id=flow_id, reverse=True
    #    )
    route_groups_map = {}
    for fid in flows_map.keys():
        route_groups_map[fid] = dfcx_route_groups.get_route_groups_map(
            flow_id=fid
        )
    webhooks_map = dfcx_webhooks.get_webhooks_map(agent_id=agent_id)
    intents_map = dfcx_intents.get_intents_map(agent_id=agent_id)
    # intents_map_rev = dfcx_intents.get_intents_map(
    #     agent_id=agent_id, reverse=True)
    entities_map = dfcx_entities.get_entities_map(agent_id=agent_id)

    print("Loading agent data...")

    # Get CX object data
    flow_data = get_all_flow_data(dfcx_flows, agent_id)
    page_data = get_all_page_data(flows_map, dfcx_pages)
    route_group_data = get_all_route_group_data(flows_map, dfcx_route_groups)
    webhook_data = dfcx_webhooks.list_webhooks(agent_id=agent_id)
    intent_data = dfcx_intents.list_intents(agent_id=agent_id)
    entity_data = dfcx_entities.list_entity_types(agent_id=agent_id)

    print("Agent data loaded.")

    # Next, process these into flat tables
    intent_df = pd.concat([pd.DataFrame({
        'date': [curr_date],
        'agentId': [agent_id],
        'agentName': [agent_name],
        'intentId': [data.name],
        'intentName': [data.display_name],
        'description': [data.description],
        'parameters': [[json.dumps({"id": param.id, "entity_type": param.entity_type, "is_list": param.is_list}) for param in data.parameters]],
        'labels': [list(data.labels.keys())]
    }) for data in intent_data])
    intent_df['date'] = pd.to_datetime(intent_df['date'])
    print('Intents:', intent_df.shape)

    # Get all training phrases (with annotations)
    # First get the full training phrase data, which is split into parts (so more rows than the number of phrases)
    intents_df = dfcx_intents.bulk_intent_to_df(agent_id, mode='advanced')

    # Process the training phrase parts
    intent_ids = []
    intent_display_names = []
    phrases = []
    annotated_phrases = []
    current_tp_index = -1
    current_intent_id = ''
    current_intent_display_name = ''
    current_phrase = ''
    current_annotated_phrase = ''
    for i, row in intents_df.iterrows():
        if row['training_phrase_idx'] != current_tp_index:
            if current_tp_index != -1:
                intent_ids.append(current_intent_id)
                intent_display_names.append(current_intent_display_name)
                phrases.append(current_phrase.strip())
                annotated_phrases.append(current_annotated_phrase.strip())
            current_tp_index = row['training_phrase_idx']
            current_intent_id = row['name']
            current_intent_display_name = row['display_name']
            current_phrase = ''
            current_annotated_phrase = ''
        current_phrase += str(row['text']) + ' '
        if not pd.isna(row['parameter_id']):
            entity_type = entities_map[row['entity_type']
                                       ] if row['entity_type'] in entities_map else row['entity_type'].split('/')[-1]
            current_annotated_phrase += '[' + str(row['text']) + ']{@' + str(
                entity_type) + ' ' + str(row['parameter_id']) + '} '
        else:
            current_annotated_phrase += str(row['text']) + ' '
    tp_count = len(phrases)
    tp_df = pd.DataFrame({
        'date': [curr_date for _ in range(tp_count)],
        'agentId': [agent_id for _ in range(tp_count)],
        'agentName': [agent_name for _ in range(tp_count)],
        'intentId': intent_ids,
        'intentName': intent_display_names,
        'phrase': phrases,
        'annotatedPhrase': annotated_phrases
    })
    tp_df['date'] = pd.to_datetime(tp_df['date'])
    print('Training phrases:', tp_df.shape)

    entity_df = pd.concat([pd.DataFrame({
        'date': [curr_date],
        'agentId': [agent_id],
        'agentName': [agent_name],
        'entityTypeId': [data.name],
        'entityTypeName': [data.display_name],
        'entity': [entity.value],
        'synonym': [synonym],
    }) for data in entity_data for entity in data.entities for synonym in entity.synonyms])
    entity_df['date'] = pd.to_datetime(entity_df['date'])
    print('Entities:', entity_df.shape)

    webhook_df = pd.concat([pd.DataFrame({
        'date': [curr_date],
        'agentId': [agent_id],
        'agentName': [agent_name],
        'webhookId': [data.name],
        'webhookName': [data.display_name],
        'timeout': [str(data.timeout)],
        'serviceDirectory': [data.service_directory.service if getattr(data, 'service_directory', None) else None],
        'url': [data.service_directory.generic_web_service.uri if getattr(data, 'service_directory', None) else data.generic_web_service.uri]
    }) for data in webhook_data])
    webhook_df['date'] = pd.to_datetime(webhook_df['date'])
    print('Webhooks:', webhook_df.shape)

    flow_df_list = []
    page_df_list = []
    parameter_df_list = []
    transition_df_list = []
    route_group_df_list = []
    for flow_id in flows_map:
        # Flows
        page_ids = [flow_id] + list(pages_map[flow_id].keys())
        flow_df = pd.DataFrame({
            'date': [curr_date],
            'agentId': [agent_id],
            'agentName': [agent_name],
            'flowId': [flow_id],
            'flowName': [flows_map[flow_id]],
            'pages': [page_ids]
        })
        flow_df_list.append(flow_df)

        # Pages
        # Start page is an exception as always
        data = flow_data[flow_id]
        # TODO: flow IDs aren't really page IDs...
        routes = parse_routes(data.transition_routes, agent_id, agent_name, flow_id, flows_map,
                              pages_map, route_groups_map, webhooks_map, intents_map, page_id=flow_id)
        event_handlers = parse_routes(data.event_handlers, agent_id, agent_name, flow_id,
                                      flows_map, pages_map, route_groups_map, webhooks_map, intents_map, page_id=flow_id)
        transition_df_list.append(routes)
        transition_df_list.append(event_handlers)
        route_ids = list(routes['transitionRouteId']) + \
            list(event_handlers['transitionRouteId'])
        route_groups = list(data.transition_route_groups)  # IDs
        new_page = pd.DataFrame({
            'date': [curr_date],
            'agentId': [agent_id],
            'agentName': [agent_name],
            'flowId': [flow_id],
            'flowName': [flows_map[flow_id]],
            'pageId': [data.name],
            'pageName': ['Start'],
            'webhookId': [None],
            'webhookName': [None],
            'webhookTag': [None],
            'fulfillment': [[]],
            'partialResponse': [False],
            'parameterPresets': [[]],
            'parameters': [[]],
            'routes': [route_ids],
            'routeGroups': [route_groups]})
        page_df_list.append(new_page)
        # Pages other than the start page
        for page_id in pages_map[flow_id]:
            if 'START_PAGE' in page_id or 'END_SESSION' in page_id or 'END_FLOW' in page_id:
                continue
            data = page_data[flow_id][page_id]
            fulfillment = parse_fulfillment(
                data.entry_fulfillment, webhooks_map)
            parameters, parameter_routes = parse_parameters(
                data.form, agent_id, agent_name, flow_id, page_id, flows_map, pages_map, route_groups_map, webhooks_map, intents_map, entities_map)
            parameter_df_list.append(parameters)
            parameter_ids = list(parameters['parameterId'])
            routes = parse_routes(data.transition_routes, agent_id, agent_name, flow_id, flows_map,
                                  pages_map, route_groups_map, webhooks_map, intents_map, page_id=page_id)
            event_handlers = parse_routes(data.event_handlers, agent_id, agent_name, flow_id,
                                          flows_map, pages_map, route_groups_map, webhooks_map, intents_map, page_id=page_id)
            transition_df_list.append(routes)
            transition_df_list.append(event_handlers)
            transition_df_list.append(parameter_routes)
            route_ids = list(routes['transitionRouteId']) + list(
                event_handlers['transitionRouteId']) + list(parameter_routes['transitionRouteId'])
            route_groups = list(data.transition_route_groups)  # IDs
            new_page = pd.DataFrame({
                'date': [curr_date],
                'agentId': [agent_id],
                'agentName': [agent_name],
                'flowId': [flow_id],
                'flowName': [flows_map[flow_id]],
                'pageId': [data.name],
                'pageName': [data.display_name],
                'webhookId': [fulfillment['webhookId']],
                'webhookName': [fulfillment['webhookName']],
                'webhookTag': [fulfillment['webhookTag']],
                'fulfillment': [fulfillment['messages']],
                'partialResponse': [fulfillment['partialResponse']],
                'parameterPresets': [fulfillment['parameterPresets']],
                'parameters': [parameter_ids],
                'routes': [route_ids],
                'routeGroups': [route_groups]})
            page_df_list.append(new_page)
        for route_group_id in route_group_data[flow_id]:
            route_group = route_group_data[flow_id][route_group_id]
            routes = parse_routes(route_group.transition_routes, agent_id, agent_name, flow_id, flows_map,
                                  pages_map, route_groups_map, webhooks_map, intents_map, route_group_id=route_group_id)
            transition_df_list.append(routes)
            route_ids = list(routes['transitionRouteId'])
            route_group_df = pd.DataFrame({
                'date': [curr_date],
                'agentId': [agent_id],
                'agentName': [agent_name],
                'flowId': [flow_id],
                'flowName': [flows_map[flow_id]],
                'routeGroupId': [data.name],
                'routeGroupName': [data.display_name],
                'routes': [route_ids]
            })
            route_group_df_list.append(route_group_df)

    flow_df = pd.concat(flow_df_list)
    flow_df['date'] = pd.to_datetime(flow_df['date'])
    print('Flows:', flow_df.shape)

    page_df = pd.concat(page_df_list)
    page_df['date'] = pd.to_datetime(page_df['date'])
    print('Pages:', page_df.shape)

    parameter_df = pd.concat(parameter_df_list)
    parameter_df['date'] = pd.to_datetime(parameter_df['date'])
    print('Parameters:', parameter_df.shape)

    transition_route_df = pd.concat(transition_df_list)
    transition_route_df['date'] = pd.to_datetime(transition_route_df['date'])
    print('Routes:', transition_route_df.shape)

    route_group_df = pd.concat(route_group_df_list)
    route_group_df['date'] = pd.to_datetime(route_group_df['date'])
    print('Route Groups:', route_group_df.shape)

    # Create overall structure junction table
    """
    structure_df_list = []
    for _, page_row in page_df.iterrows():
        structure_df_row = pd.DataFrame({
            'date': [curr_date],
            'agentId': [agent_id],
            'flowId': [page_row['flowId']],
            'pageId': [page_row['pageId']],
            # If I add these, it will probably be too big...
            #'routeGroupId': [None],
            #'parameterId': [None],
            #'transitionRouteId': [None]
        })
        structure_df_list.append(structure_df_row)
    structure_df = pd.concat(structure_df_list)
    print('Structure:', structure_df.shape)
    """

    return {
        'Intents': intent_df,
        'TrainingPhrases': tp_df,
        'Entities': entity_df,
        'Webhooks': webhook_df,
        'Pages': page_df,
        'Flows': flow_df,
        'TransitionRoutes': transition_route_df,
        'RouteGroups': route_group_df,
        'Parameters': parameter_df,
        # 'Structure': structure_df
    }


def write_to_bq(agent_data, project_id):
    # Pages
    pages_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "fulfillment",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "partialResponse",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "parameterPresets",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "parameters",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "routes",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "routeGroups",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    pages_table_id = 'agent_structure.pages'
    print(
        f"Writing data to Bigquery table {pages_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Pages'], pages_table_id, project_id=project_id,
                      if_exists='append', table_schema=pages_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {pages_table_id} in project {project_id}")

    # Flows
    flows_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "pages",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    flows_table_id = 'agent_structure.flows'
    print(
        f"Writing data to Bigquery table {flows_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Flows'], flows_table_id, project_id=project_id,
                      if_exists='append', table_schema=flows_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {flows_table_id} in project {project_id}")

    # Intents
    intents_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "parameters",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "labels",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    intents_table_id = 'agent_structure.intents'
    print(
        f"Writing data to Bigquery table {intents_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Intents'], intents_table_id, project_id=project_id,
                      if_exists='append', table_schema=intents_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {intents_table_id} in project {project_id}")

    # Training Phrases
    tp_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        }
    ]

    tp_table_id = 'agent_structure.training_phrases'
    print(
        f"Writing data to Bigquery table {tp_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['TrainingPhrases'], tp_table_id, project_id=project_id,
                      if_exists='append', table_schema=tp_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {tp_table_id} in project {project_id}")

    # Entities
    entities_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        }
    ]

    entities_table_id = 'agent_structure.entity_types'
    print(
        f"Writing data to Bigquery table {entities_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Entities'], entities_table_id, project_id=project_id,
                      if_exists='append', table_schema=entities_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {entities_table_id} in project {project_id}")

    # Webhooks
    webhooks_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        }
    ]

    webhooks_table_id = 'agent_structure.webhooks'
    print(
        f"Writing data to Bigquery table {webhooks_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Webhooks'], webhooks_table_id, project_id=project_id,
                      if_exists='append', table_schema=webhooks_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {webhooks_table_id} in project {project_id}")

    # Route Groups
    route_groups_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "routes",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    route_groups_table_id = 'agent_structure.transition_route_groups'
    print(
        f"Writing data to Bigquery table {route_groups_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['RouteGroups'], route_groups_table_id, project_id=project_id,
                      if_exists='append', table_schema=route_groups_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {route_groups_table_id} in project {project_id}")

    # Parameters
    parameters_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "required",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "isList",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "redactInLog",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "fulfillment",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "partialResponse",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "parameterPresets",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "routes",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    parameters_table_id = 'agent_structure.parameters'
    print(
        f"Writing data to Bigquery table {parameters_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['Parameters'], parameters_table_id, project_id=project_id,
                      if_exists='append', table_schema=parameters_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {parameters_table_id} in project {project_id}")

    # Transition Routes
    transition_routes_schema = [
        {
            "name": "date",
            "type": "DATETIME",
            "mode": "NULLABLE"
        },
        {
            "name": "fulfillment",
            "type": "STRING",
            "mode": "REPEATED"
        },
        {
            "name": "partialResponse",
            "type": "BOOLEAN",
            "mode": "NULLABLE"
        },
        {
            "name": "parameterPresets",
            "type": "STRING",
            "mode": "REPEATED"
        }
    ]

    transition_routes_table_id = 'agent_structure.transition_routes'
    print(
        f"Writing data to Bigquery table {transition_routes_table_id} in project {project_id}")
    pandas_gbq.to_gbq(agent_data['TransitionRoutes'], transition_routes_table_id, project_id=project_id,
                      if_exists='append', table_schema=transition_routes_schema, progress_bar=False)
    print(
        f"Done writing to Bigquery table {transition_routes_table_id} in project {project_id}")


def main(event, context=None):
    print('Starting agent structure logger')

    try:
        data = base64.b64decode(event['data']).decode('utf-8')
        message_data = json.loads(data)

        # Extract agent data from message_data
        for agent in message_data:
            agent_project_id = agent["agent_project_id"]
            agent_location = agent["agent_location"]
            agent_id = agent["agent_id"]
            bq_project_id = agent["bq_project_id"]
            full_agent_path = f"projects/{agent_project_id}/locations/{agent_location}/agents/{agent_id}"

            # Get agent name
            dfcx_a = Agents()
            agent_name = dfcx_a.get_agent(
                agent_id=full_agent_path).display_name

            # Get agent data and parse
            print("Loading agent: ", agent_name)
            agent_data = load_agent_data(
                full_agent_path, agent_name)

            # Write agent information to BQ
            write_to_bq(agent_data, bq_project_id)

    except Exception as e:
        print(f"Error: {e}")
        return "Error", 500

    return 'Success', 200
