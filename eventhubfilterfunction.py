import json
import logging
import asyncio
import azure.functions as func
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

app = func.FunctionApp()

KEYVAULT_VAULTS_EVENTTYPE = 'microsoft.keyvault/vaults'
ROLEASSIGNMENTS_EVENTTYPE = 'microsoft.authorization/roleassignments'

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="",
                               connection="")
def eventhub_trigger(azeventhub: func.EventHubEvent):
    try:
        # Decode the event body (assuming it's in JSON format)
        input_event_data = json.loads(azeventhub.get_body().decode('utf-8'))
        records = input_event_data['records']
        isRoleAssignmentEvent = False
        isKeyVaultEvent = False
        i = 0
        while i < len(records):
            record = records[i]
            if record['category'] == 'Administrative' and ROLEASSIGNMENTS_EVENTTYPE in record['operationName'].lower():
                print('Role Assignment Event: %s', record)
                isRoleAssignmentEvent = True
            elif record['category'] == 'AuditEvent' and KEYVAULT_VAULTS_EVENTTYPE in record['resourceId'].lower():
                print('Keyvault Event: %s', record)
                isKeyVaultEvent = True
            else:
                print('Non-Role Assignment or KeyVault Event: %s', record)
            i += 1
        # event_body = event_dataToSend.body.decode('utf-8')
        if isRoleAssignmentEvent or isKeyVaultEvent:
            asyncio.run(send_events(json.dumps(input_event_data)))
    except Exception as e:
        logging.error('Error processing event: %s', str(e))

async def send_events(event):
    # Create a producer client to send messages to the event hub
    producer_client = EventHubProducerClient.from_connection_string(
        conn_str="",
        eventhub_name=""
    )

    try:
        # Create an event data object
        event_data = EventData(event)

        # Send the event data
        await producer_client.send_batch([event_data])
        print("Event sent successfully!")
    finally:
        await producer_client.close()