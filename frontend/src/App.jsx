import React, { useState } from "react";
import FileUploader from "./components/FileUploader";
import OutputDownloader from "./components/OutputDownloader";

export default function App() {
  const [outputs, setOutputs] = useState([]);

  return (
    <div>
      <h1>QEM Repository Extractor</h1>
      <FileUploader setOutputs={setOutputs} />
      {outputs.length > 0 && <OutputDownloader outputs={outputs} />}
    </div>
  );
}
