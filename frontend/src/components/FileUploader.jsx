import React, { useState } from "react";
import axios from "axios";

export default function FileUploader({ setOutputs }) {
  const [jsonFiles, setJsonFiles] = useState([]);
  const [tsvFile, setTsvFile] = useState(null);

  const handleSubmit = async () => {
    const formData = new FormData();
    jsonFiles.forEach(file => formData.append("json_files", file));
    formData.append("tsv_file", tsvFile);

    const response = await axios.post("http://localhost:8000/extract/run", formData, {
      headers: { "Content-Type": "multipart/form-data" },
    });
    setOutputs(response.data.outputs);
  };

  return (
    <div>
      <h3>Upload Files</h3>
      <input type="file" multiple onChange={e => setJsonFiles([...e.target.files])} />
      <input type="file" onChange={e => setTsvFile(e.target.files[0])} />
      <button onClick={handleSubmit}>Run Extraction</button>
    </div>
  );
}
