import React from "react";

export default function OutputDownloader({ outputs }) {
  return (
    <div>
      <h3>Download Outputs</h3>
      {outputs.map(file => (
        <div key={file}>
          <a href={`http://localhost:8000/download/${file}`} target="_blank" rel="noreferrer">
            {file}
          </a>
        </div>
      ))}
    </div>
  );
}
