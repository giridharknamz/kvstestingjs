// upload.js
const { KinesisVideoClient, GetDataEndpointCommand } = require("@aws-sdk/client-kinesis-video");
const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
const ffmpeg = require('fluent-ffmpeg')
ffmpeg.setFfmpegPath(ffmpegPath)
const aws4 = require('aws4')
const axios = require('axios')
const CancelToken = axios.CancelToken
const fs = require('fs')
const PipeViewer = require('pv')
function noop() { }

const HANGING_TIMEOUT = 20 * 1000

function getMkvStream(uri, onStart, onFinished) {
    let processHanging = null

    const audio = ffmpeg(uri)
        .format('matroska')
        .outputOptions(['-ar 8000', '-acodec pcm_s16le'
        ])        
        .on('progress', function (progress) {
            console.log('Processing:', progress)
            clearTimeout(processHanging)
            processHanging = setTimeout(function () {
                console.error('Hanging for more than 20 sec.')
                audio.kill()
            }, HANGING_TIMEOUT) // hanging for more than x sec
        })
        .on('start', function (commandLine) {
            console.log('Spawned Ffmpeg with command: ' + commandLine)
            onStart && onStart()
        })
        .on('end', function () {
            console.log('Finished processing')
            onFinished && onFinished()
        })
        .on('error', function (err, stdout, stderr) {
            console.log('Cannot process video: ' + err.message)
            clearTimeout(processHanging)
            onFinished && onFinished(err) // eventually restart
        })

    return audio
}

function sendStream({ mkvFile, streamName, dataEndpoint, pwd }, onStart, onFinish) {
    onFinish = onFinish || noop

    const opts = {
        service: 'kinesisvideo',
        host: dataEndpoint,
        path: '/putMedia',
        method: 'POST', // NB. required!
        headers: {
            'Content-Type': 'application/json',
            'x-amzn-stream-name': streamName,
            "x-amzn-fragment-timecode-type": "ABSOLUTE",
            "x-amz-content-sha256": "UNSIGNED-PAYLOAD"
        }
    }

    const signed = aws4.sign(opts, pwd)

    console.log(signed)

    const reqUri = 'https://' + opts.host + opts.path

    let cancel;

    axios({
        method: 'POST',
        timeout: 40 * 1000,
        url: reqUri,
        headers: signed.headers,
        data: mkvFile,
        responseType: 'stream',
        maxContentLength: Infinity, // required for stream!
        cancelToken: new CancelToken(function executor(c) {
            // An executor function receives a cancel function as a parameter
            cancel = c
        })
    }).then(function (res) {
        onStart && onStart(cancel)
        res.data.on('data', function (fragRes) {
            console.log(fragRes.toString())
            fragRes = JSON.parse(fragRes.toString())
            const EventType = fragRes.EventType
            const ErrorCode = fragRes.ErrorCode
            if (EventType === 'ERROR') {
                // request aborted
                onFinish(new Error('Fragment event type: ERROR - ' + ErrorCode))
            }
        })
    }).catch(onFinish)

}

function startUpload({ audioUri, streamName, dataEndpoint, pwd }) {
    // get stream
    const ffmpegAudio = getMkvStream(audioUri, null, function (err) {
        if (err) {
            startUpload({ audioUri, streamName, dataEndpoint }) // start again
        }
    })
    const pv = PipeViewer()
    pv.on('info', function (str) {
        console.log('Speed: ' + str.speed + ' - Transferred: ' + str.transferred)
    })
    // start send stream
    sendStream({ mkvFile: ffmpegAudio.pipe(pv), streamName, dataEndpoint, pwd }, function started(cancel) {
        setTimeout(function () {
            cancel() // stop upload
            ffmpegAudio.kill() // stop stream and raise err cb above
        }, 40 * 60 * 1000) // 40 min
    }, function finished(err) {
        if (err) console.error(err)
    })
}

// main
async function main() {
    // node upload.js ./samples/input.wav
    const args = process.argv.slice(2);
    console.log(args)

    // getMkvStream(args[0]).pipe().pipe(require('fs').createWriteStream('./test.mkv'), null, console.error)

    const STREAM_NAME = 'testkvsstream1';
    const config = { region: 'us-west-2' };
    const input = {
        APIName: 'PUT_MEDIA',
        StreamName: STREAM_NAME
    }

    const client = new KinesisVideoClient(config);
    const command = new GetDataEndpointCommand(input);
    const response = await client.send(command);

    console.log({ response });

    startUpload({
        audioUri: args[0],
        streamName: STREAM_NAME,
        dataEndpoint: response.DataEndpoint.split('://')[1],
        pwd: {
            endpoint: 'kinesisvideo.us-east-1.amazonaws.com',
            region: 'us-west-2',
            accessKeyId: "",
            secretAccessKey: "",
        }
    })
}

main();