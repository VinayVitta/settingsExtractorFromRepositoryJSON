import React from "react";
import FileUploader from "./components/uploader/FileUploader";

export default function App() {
  return (
    <div className="min-h-screen bg-gray-50 flex flex-col items-center p-6 font-sans">
      {/* Header Section */}
      <header className="w-full max-w-5xl bg-white shadow-md rounded-xl px-8 py-5 mb-8 border-l-8 border-[#a0a0a0]">
        <h1 className="text-4xl font-extrabold text-[#a0a0a0] tracking-tight">
          QDI - PS
        </h1>
        <p className="text-lg text-gray-600 mt-1">
          Qlik Data Integration Professional Services Tool
        </p>
      </header>

      {/* Main Content */}
      <main className="w-full max-w-5xl">
        <FileUploader />
      </main>
    </div>
  );
}
