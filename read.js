const { KinesisVideoClient, GetDataEndpointCommand } = require("@aws-sdk/client-kinesis-video");
const { KinesisVideoMediaClient, GetMediaCommand } = require("@aws-sdk/client-kinesis-video-media");

// main
async function main() {
    // node upload.js ./samples/input.wav
    const args = process.argv.slice(2);
    console.log(args)

    // getMkvStream(args[0]).pipe().pipe(require('fs').createWriteStream('./test.mkv'), null, console.error)

    const STREAM_NAME = 'testkvsstream1';
    const config = { region: 'us-west-2' };
    const input = {
        APIName: 'GET_MEDIA',
        StreamName: STREAM_NAME
    }
    const getMediaInput = { // GetMediaInput
        StreamName: STREAM_NAME,
        StartSelector: { // StartSelector
          StartSelectorType: "EARLIEST", // required
        },
      };

    const client = new KinesisVideoClient(config);
    const command = new GetDataEndpointCommand(input);
    const response = await client.send(command);
  
    console.log({ response });

    const mediaClient = new KinesisVideoMediaClient({...config, endpoint: response.DataEndpoint});
    const getMediaCommand = new GetMediaCommand(getMediaInput);
    const getMediaResponse = await mediaClient.send(getMediaCommand);
    console.log({ getMediaResponse });
    const streamReader = getMediaResponse.Payload;

    let counter = 0;
    let totalSize = 0;
    try {
        for await (const chunk of streamReader) {
            totalSize += chunk.length;
            console.log(`Read chunk ${counter++}, size = ${totalSize}`);
        }
    } catch (error) {
        console.error(`Error processing chunk ${counter}, error = ${error}`);
    } finally {
        console.log(`Done reading chunks`);
    }
}

main();