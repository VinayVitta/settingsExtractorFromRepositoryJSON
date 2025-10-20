import React, { useEffect, useState } from "react";

// 🎨 Define styles outside the component for cleaner JSX
const styles = {
  container: {
    marginTop: "40px",
    padding: "20px",
    border: "1px solid #e0e0e0",
    borderRadius: "8px",
    boxShadow: "0 2px 4px rgba(0, 0, 0, 0.05)",
    backgroundColor: "#ffffff",
  },
  heading: {
    marginBottom: "15px",
    color: "#4285F4", // Google Blue
    borderBottom: "2px solid #f0f0f0",
    paddingBottom: "10px",
    fontWeight: 500,
  },
  listContainer: {
    display: "flex",
    gap: "50px",
    marginTop: "20px",
  },
  listSection: {
    flex: 1,
    padding: "15px",
    borderRadius: "6px",
    backgroundColor: "#fafafa",
  },
  subHeading: {
    marginBottom: "10px",
    color: "#34a853", // Google Green
    borderBottom: "1px solid #e0e0e0",
    paddingBottom: "5px",
    fontWeight: 600,
    fontSize: "1.1em",
  },
  list: {
    listStyleType: "none", // Remove default bullets
    paddingLeft: "0",
  },
  listItem: {
    padding: "4px 0",
    color: "#333",
    borderBottom: "1px dotted #eee", // Subtle separator
  },
};

export default function SupportedSourcesTargets() {
  const [sources, setSources] = useState([]);
  const [targets, setTargets] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const apiBase = process.env.REACT_APP_API_BASE_URL;

  useEffect(() => {
    fetch(`${apiBase}/info/supported`)
      .then((res) => {
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        return res.json();
      })
      .then((data) => {
        setSources(data.sources || []);
        setTargets(data.targets || []);
      })
      .catch((err) => {
        console.error("Error fetching supported sources/targets:", err);
        setError("Could not load supported components. Is the API running?");
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, []);

  // --- UX Handlers ---

  if (isLoading) {
    return (
      <div style={{ padding: "20px", color: "#666" }}>
        🚀 **Loading** supported components...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ padding: "20px", color: "#d93025", border: "1px solid #fce8e6", borderRadius: "4px", backgroundColor: "#fef7f7" }}>
        ⚠️ **Error:** {error}
      </div>
    );
  }

  // Hide the component entirely if no data is found (even after loading)
  if (!sources.length && !targets.length) {
    return (
      <div style={{ padding: "20px", color: "#a0a0a0" }}>
        ℹ️ No supported sources or targets were returned from the API.
      </div>
    );
  }

  // --- Render Content ---

  const renderList = (items) => (
    <ul style={styles.list}>
      {items.map((item, i) => (
        <li key={i} style={styles.listItem}>
          {item}
        </li>
      ))}
    </ul>
  );

  return (
    <div style={styles.container}>
      <h3 style={styles.heading}>🔗 Supported Data Connectors</h3>
      <div style={styles.listContainer}>
        <div style={styles.listSection}>
          <h4 style={styles.subHeading}>Source Connectors (Data Input)</h4>
          {sources.length > 0 ? renderList(sources) : <p style={{ color: "#a0a0a0" }}>None configured.</p>}
        </div>
        <div style={styles.listSection}>
          <h4 style={styles.subHeading}>Target Connectors (Data Output)</h4>
          {targets.length > 0 ? renderList(targets) : <p style={{ color: "#a0a0a0" }}>None configured.</p>}
        </div>
      </div>
    </div>
  );
}