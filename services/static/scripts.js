
// const fetchDataBtn = document.getElementById('fetchDataBtn');
// const dataTable = document.getElementById('dataTable').getElementsByTagName('tbody')[0];

// fetchDataBtn.addEventListener('click', async () => {
//     const response = await fetch('/trace/get_table_of_contents');
//     const data = await response.json();
//     displayData(data.result);
// });

// // function displayData(data) {
// //     dataTable.innerHTML = '';

// //     for (const item of data) {
// //         const row = dataTable.insertRow();

// //         const titleCell = row.insertCell();
// //         titleCell.innerText = item.Title;
// //         titleCell.classList.add('title'); // Add a class to the title cells
// //         titleCell.dataset.filename = item.Title; // Store the filename in a data attribute

// //         row.insertCell().innerText = item.CHANNEL;
// //         row.insertCell().innerText = item.Handle;
// //         row.insertCell().innerText = item.LOCATION;
// //         row.insertCell().innerText = item.EMOTION.join(', ');
// //         row.insertCell().innerText = item.SITUATIONS.join(', ');
// //         row.insertCell().innerText = item.Virality.toFixed(2);
// //         row.insertCell().innerText = item.VIEWS;
// //         row.insertCell().innerText = item.REACTS;
// //         row.insertCell().innerText = item.PErson_Location_detected;
// //         row.insertCell().innerText = item.Datetime;
// //         row.insertCell().innerText = item.Threat.toFixed(2);
// //     }

// //     // Add event listeners to the title cells
// //     const titleCells = document.getElementsByClassName('title');
// //     for (const titleCell of titleCells) {
// //         titleCell.addEventListener('click', async () => {
// //             const filename = titleCell.dataset.filename;
// //             const response = await fetch(`/trace/get_video_info?filename=${filename}`);
// //             const videoInfo = await response.json();
// //             console.log(videoInfo);
// //             // Display the video details as required
// //             titleCell.style.cursor = 'pointer';
// //             titleCell.addEventListener('click', () => {
// //                 window.location.href = `/video_details/${item.Title}`;
// //             });
// //         });
        
// //     }
// // }

// function displayData(data) {
//     dataTable.innerHTML = '';

//     for (const item of data) {
//         const row = dataTable.insertRow();

//         const titleCell = row.insertCell();
//         titleCell.innerText = item.Title;
//         titleCell.classList.add('title'); // Add a class to the title cells
//         titleCell.dataset.filename = item.Title; // Store the filename in a data attribute
//         titleCell.style.cursor = 'pointer'; // Set the cursor style for the title cells

//         row.insertCell().innerText = item.CHANNEL;
//         row.insertCell().innerText = item.Handle;
//         row.insertCell().innerText = item.LOCATION;
//         row.insertCell().innerText = item.EMOTION.join(', ');
//         row.insertCell().innerText = item.SITUATIONS.join(', ');
//         row.insertCell().innerText = item.Virality.toFixed(2);
//         row.insertCell().innerText = item.VIEWS;
//         row.insertCell().innerText = item.REACTS;
//         row.insertCell().innerText = item.PErson_Location_detected;
//         row.insertCell().innerText = item.Datetime;
//         row.insertCell().innerText = item.Threat.toFixed(2);
//     }

//     // Add event listeners to the title cells
//     const titleCells = document.getElementsByClassName('title');
//     for (const titleCell of titleCells) {
//         titleCell.addEventListener('click', () => {
//             const filename = titleCell.dataset.filename;
//             window.location.href = `/trace/get_video_info?filename=${filename}`;
//         });
//     }
// }


// async function fetchVideoDetails(filename) {
//     const response = await fetch(`/trace/get_video_info?filename=${filename}`);
//     const data = await response.json();
//     displayVideoDetails(data);
//   }
  
  
// function displayVideoDetails(data) {
//     const videoDetailsDiv = document.getElementById('videoDetails');
//     // Display the details in the desired format
//   }
  
//   if (window.location.pathname.includes('/video_details/')) {
//     const filename = window.location.pathname.split('/').pop();
//     fetchVideoDetails(filename);
//   }

//   function displayVideoDetails(data) {
//     const videoDetailsDiv = document.getElementById('videoDetails');

//     // Display Quality Analysis
//     const qualityAnalysis = document.createElement('div');
//     qualityAnalysis.innerHTML = '<h2>Quality Analysis</h2>';
//     const qualityList = document.createElement('ul');
//     for (const key in data.QUALITY_ANALYSIS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.QUALITY_ANALYSIS[key]}`;
//         qualityList.appendChild(listItem);
//     }
//     qualityAnalysis.appendChild(qualityList);
//     videoDetailsDiv.appendChild(qualityAnalysis);

//     // Display Detected Watermarks
//     const detectedWatermarks = document.createElement('div');
//     detectedWatermarks.innerHTML = '<h2>Detected Watermarks</h2>';
//     const watermarksList = document.createElement('ul');
//     for (const key in data.DETECTED_WATERMARKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.DETECTED_WATERMARKS[key]}`;
//         watermarksList.appendChild(listItem);
//     }
//     detectedWatermarks.appendChild(watermarksList);
//     videoDetailsDiv.appendChild(detectedWatermarks);

//     // Display Sanity Checks
//     const sanityChecks = document.createElement('div');
//     sanityChecks.innerHTML = '<h2>Sanity Checks</h2>';
//     const sanityList = document.createElement('ul');
//     for (const key in data.SANITY_CHECKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.SANITY_CHECKS[key]}`;
//         sanityList.appendChild(listItem);
//     }
//     sanityChecks.appendChild(sanityList);
//     videoDetailsDiv.appendChild(sanityChecks);

//     // Display Audio Audit
//     const audioAudit = document.createElement('div');
//     audioAudit.innerHTML = '<h2>Audio Audit</h2>';
//     const audioList = document.createElement('ul');
//     for (const key in data.AUDIO_AUDIT) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.AUDIO_AUDIT[key]}`;
//         audioList.appendChild(listItem);
//     }
//     audioAudit.appendChild(audioList);
//     videoDetailsDiv.appendChild(audioAudit);

//     // Display Video Audit
//     const videoAudit = document.createElement('div');
//     videoAudit.innerHTML = '<h2>Video Audit</h2>';
//     const videoList = document.createElement('ul');
//     for (const key in data.VIDEO_AUDIT) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.VIDEO_AUDIT[key]}`;
//         videoList.appendChild(listItem);
//     }
//     videoAudit.appendChild(videoList);
//     videoDetailsDiv.appendChild(videoAudit);
// }


  
const fetchDataBtn = document.getElementById('fetchDataBtn');
const dataTable = document.getElementById('dataTable').getElementsByTagName('tbody')[0];

fetchDataBtn.addEventListener('click', async () => {
    const response = await fetch('/trace/get_table_of_contents');
    const data = await response.json();
    displayData(data.result);
});

function displayData(data) {
    dataTable.innerHTML = '';

    for (const item of data) {
        const row = dataTable.insertRow();

        const titleCell = row.insertCell();
        titleCell.innerText = item.Title;
        titleCell.classList.add('title'); // Add a class to the title cells
        titleCell.dataset.filename = item.Title; // Store the filename in a data attribute
        titleCell.style.cursor = 'pointer'; // Set the cursor style for the title cells

        row.insertCell().innerText = item.CHANNEL;
        row.insertCell().innerText = item.Handle;
        row.insertCell().innerText = item.LOCATION;
        row.insertCell().innerText = item.EMOTION.join(', ');
        row.insertCell().innerText = item.SITUATIONS.join(', ');
        row.insertCell().innerText = item.Virality.toFixed(2);
        row.insertCell().innerText = item.VIEWS;
        row.insertCell().innerText = item.REACTS;
        row.insertCell().innerText = item.PErson_Location_detected;
        row.insertCell().innerText = item.Datetime;
        row.insertCell().innerText = item.Threat.toFixed(2);
    }

    // Add event listeners to the title cells
    const titleCells = document.getElementsByClassName('title');
    for (const titleCell of titleCells) {
        titleCell.addEventListener('click', () => {
            const filename = titleCell.dataset.filename;
            window.location.href = `/trace/video_details/${filename}`;
            // window.location.href = `/trace/get_video_info?filename=${filename}`;

        });
    }
}   

async function fetchVideoDetails(filename) {
    const response = await fetch(`/trace/get_video_info?filename=${filename}`);
    const data = await response.json();
    displayVideoDetails(data);
}

// function displayVideoDetails(data) {
//     const videoDetailsDiv = document.getElementById('videoDetails');

//     // Display Quality Analysis
//     const qualityAnalysis = document.createElement('div');
//     qualityAnalysis.innerHTML = '<h2>Quality Analysis</h2>';
//     const qualityList = document.createElement('ul');
//     for (const key in data.QUALITY_ANALYSIS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.QUALITY_ANALYSIS[key]}`;
//         qualityList.appendChild(listItem);
//     }
//     qualityAnalysis.appendChild(qualityList);
//     videoDetailsDiv.appendChild(qualityAnalysis);

//     // Display Detected Watermarks
//     const detectedWatermarks = document.createElement('div');
//     detectedWatermarks.innerHTML = '<h2>Detected Watermarks</h2>';
//     const watermarksList = document.createElement('ul');
//     for (const key in data.DETECTED_WATERMARKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.DETECTED_WATERMARKS[key]}`;
//         watermarksList.appendChild(listItem);
//     }
//     detectedWatermarks.appendChild(watermarksList);
//     videoDetailsDiv.appendChild(detectedWatermarks);

//     // Display Sanity Checks
//     const sanityChecks = document.createElement('div');
//     sanityChecks.innerHTML = '<h2>Sanity Checks</h2>';
//     const sanityList = document.createElement('ul');
//     for (const key in data.SANITY_CHECKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.SANITY_CHECKS[key]}`;
//         sanityList.appendChild(listItem);
//     }
//     sanityChecks.appendChild(sanityList);
//     videoDetailsDiv.appendChild(sanityChecks);

//     // Display Audio Audit
//     const audioAudit = document.createElement('div');
//     audioAudit.innerHTML = '<h2>Audio Audit</h2>';
//     const audioList = document.createElement('ul');
//     for (const key in data.AUDIO_AUDIT) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.AUDIO_AUDIT[key]}`;
//         audioList.appendChild(listItem);
//     }
//     audioAudit.appendChild(audioList);
//     videoDetailsDiv.appendChild(audioAudit);

//     // Display Video Audit
//     const videoAudit = document.createElement('div');
//     videoAudit.innerHTML = '<h2>Video Audit</h2>';
//     const videoList = document.createElement('ul');
//     for (const key in data.VIDEO_AUDIT) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.VIDEO_AUDIT[key]}`;
//         videoList.appendChild(listItem);
//     }
//     videoAudit.appendChild(videoList);
//     videoDetailsDiv.appendChild(videoAudit);
// }

// function displayVideoDetails(data) {
//     const videoDetailsDiv = document.getElementById('videoDetails');

//     // Display Quality Analysis
//     const qualityAnalysis = document.createElement('div');
//     qualityAnalysis.innerHTML = '<h2>Quality Analysis</h2>';
//     const qualityList = document.createElement('ul');
//     for (const key in data.QUALITY_ANALYSIS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.QUALITY_ANALYSIS[key]}`;
//         qualityList.appendChild(listItem);
//     }
//     qualityAnalysis.appendChild(qualityList);
//     videoDetailsDiv.appendChild(qualityAnalysis);

//     // Display Detected Watermarks
//     const detectedWatermarks = document.createElement('div');
//     detectedWatermarks.innerHTML = '<h2>Detected Watermarks</h2>';
//     const watermarksList = document.createElement('ul');
//     for (const key in data.DETECTED_WATERMARKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.DETECTED_WATERMARKS[key]}`;
//         watermarksList.appendChild(listItem);
//     }
//     detectedWatermarks.appendChild(watermarksList);
//     videoDetailsDiv.appendChild(detectedWatermarks);

//     // Display Sanity Checks
//     const sanityChecks = document.createElement('div');
//     sanityChecks.innerHTML = '<h2>Sanity Checks</h2>';
//     const sanityList = document.createElement('ul');
//     for (const key in data.SANITY_CHECKS) {
//         const listItem = document.createElement('li');
//         listItem.textContent = `${key}: ${data.SANITY_CHECKS[key]}`;
//         sanityList.appendChild(listItem);
//     }
//     sanityChecks.appendChild(sanityList);
//     videoDetailsDiv.appendChild(sanityChecks);

//     // Display Audio Audit
//     const audioAudit = document.createElement('div');
//     audioAudit.innerHTML = '<h2>Audio Audit</h2>';
//     const audioList = document.createElement('ul');
//     for (const key in data.AUDIO_AUDIT) {
//         const listItem = document.createElement('li');
//         const value = data.AUDIO_AUDIT[key];
//         listItem.textContent = `${key}: ${value instanceof Object ? '[object Object]' : value}`;
//         audioList.appendChild(listItem);
//     }
//     audioAudit.appendChild(audioList);
//     videoDetailsDiv.appendChild(audioAudit);

//     // Display Video Audit
//     const videoAudit = document.createElement('div');
//     videoAudit.innerHTML = '<h2>Video Audit</h2>';
//     const videoList = document.createElement('ul');
//     for (const key in data.VIDEO_AUDIT) {
//         const listItem = document.createElement('li');
//         const value = data.VIDEO_AUDIT[key];
//         listItem.textContent = `${key}: ${value instanceof Object ? JSON.stringify(value) : value}`;
//         videoList.appendChild(listItem);
//     }
//     videoAudit.appendChild(videoList);
//     videoDetailsDiv.appendChild(videoAudit);
// }


function displayVideoDetails(data) {
    const videoDetailsDiv = document.getElementById('videoDetails');

    // Display Quality Analysis
    const qualityAnalysis = document.createElement('div');
    qualityAnalysis.innerHTML = '<h2>Quality Analysis</h2>';
    const qualityList = document.createElement('ul');
    for (const key in data.QUALITY_ANALYSIS) {
        const listItem = document.createElement('li');
        let value = data.QUALITY_ANALYSIS[key];
        if (Array.isArray(value) && value.length === 0) {
            value = 'N/A';
        }
        listItem.textContent = `${key}: ${value}`;
        qualityList.appendChild(listItem);
    }
    qualityAnalysis.appendChild(qualityList);
    videoDetailsDiv.appendChild(qualityAnalysis);

    // Display Detected Watermarks
    const detectedWatermarks = document.createElement('div');
    detectedWatermarks.innerHTML = '<h2>Detected Watermarks</h2>';
    const watermarksList = document.createElement('ul');
    for (const key in data.DETECTED_WATERMARKS) {
        const listItem = document.createElement('li');
        let value = data.DETECTED_WATERMARKS[key];
        if (Array.isArray(value) && value.length === 0) {
            value = 'N/A';
        }
        listItem.textContent = `${key}: ${value}`;
        watermarksList.appendChild(listItem);
    }
    detectedWatermarks.appendChild(watermarksList);
    videoDetailsDiv.appendChild(detectedWatermarks);

    // Display Sanity Checks
    const sanityChecks = document.createElement('div');
    sanityChecks.innerHTML = '<h2>Sanity Checks</h2>';
    const sanityList = document.createElement('ul');
    for (const key in data.SANITY_CHECKS) {
        const listItem = document.createElement('li');
        let value = data.SANITY_CHECKS[key];
        if (Array.isArray(value) && value.length === 0) {
            value = 'N/A';
        }
        listItem.textContent = `${key}: ${value}`;
        sanityList.appendChild(listItem);
    }
    sanityChecks.appendChild(sanityList);
    videoDetailsDiv.appendChild(sanityChecks);

    // Display Audio Audit
    const audioAudit = document.createElement('div');
    audioAudit.innerHTML = '<h2>Audio Audit</h2>';
    const audioList = document.createElement('ul');
    for (const key in data.AUDIO_AUDIT) {
        const listItem = document.createElement('li');
        let value = data.AUDIO_AUDIT[key];
        if (Array.isArray(value) && value.length === 0) {
            value = 'N/A';
        }
        listItem.textContent = `${key}: ${value}`;
        audioList.appendChild(listItem);
    }
    audioAudit.appendChild(audioList);
    videoDetailsDiv.appendChild(audioAudit);

    // Display Video Audit
    const videoAudit = document.createElement('div');
    videoAudit.innerHTML = '<h2>Video Audit</h2>';
    const videoList = document.createElement('ul');
    for (const key in data.VIDEO_AUDIT) {
        const listItem = document.createElement('li');
        const value = data.VIDEO_AUDIT[key];
        listItem.textContent = `${key}: ${value instanceof Object ? JSON.stringify(value) : value}`;
        videoList.appendChild(listItem);
    }
    videoAudit.appendChild(videoList);
    videoDetailsDiv.appendChild(videoAudit);
}

  

document.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.search);
    const filename = urlParams.get('filename');

    if (filename) {
        fetchVideoDetails(filename);
    }
});


