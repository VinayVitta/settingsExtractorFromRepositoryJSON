import React from "react";
import FileUploader from "./components/uploader/FileUploader";

export default function App() {
  return (
    <div className="min-h-screen bg-gray-100 flex items-center justify-center p-4">
      <div className="text-center">
        <h1 className="text-5xl font-extrabold text-indigo-700 mb-2 tracking-tight">
          QDI - PS
        </h1>
        <p className="text-xl text-gray-500 mb-10">
            Qlik Data Integration Professional Services Tool
        </p>
        <FileUploader />
      </div>
    </div>
  );
}
