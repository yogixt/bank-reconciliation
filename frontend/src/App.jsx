import { useState } from 'react';
import './App.css';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function App() {
    const [files, setFiles] = useState({
        bankStatement: null,
        bridgeFile: null,
        transactionIds: null
    });
    const [processing, setProcessing] = useState(false);
    const [progress, setProgress] = useState(0);
    const [result, setResult] = useState(null);
    const [error, setError] = useState(null);

    const handleFileChange = (fileType, file) => {
        setFiles(prev => ({ ...prev, [fileType]: file }));
        setError(null);
    };

    const handleSubmit = async () => {
        if (!files.bankStatement || !files.bridgeFile || !files.transactionIds) {
            setError('Please upload all three files to continue');
            return;
        }

        setProcessing(true);
        setError(null);
        setResult(null);
        setProgress(0);

        const progressInterval = setInterval(() => {
            setProgress(prev => {
                if (prev >= 90) {
                    clearInterval(progressInterval);
                    return prev;
                }
                return prev + 10;
            });
        }, 200);

        const formData = new FormData();
        formData.append('bank_statement', files.bankStatement);
        formData.append('bridge_file', files.bridgeFile);
        formData.append('transaction_ids', files.transactionIds);

        try {
            const response = await fetch(`${API_URL}/api/reconcile`, {
                method: 'POST',
                body: formData
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || 'Reconciliation failed');
            }

            const data = await response.json();
            clearInterval(progressInterval);
            setProgress(100);

            setTimeout(() => {
                setResult(data);
                setProcessing(false);
            }, 500);
        } catch (err) {
            clearInterval(progressInterval);
            setError(err.message);
            setProcessing(false);
            setProgress(0);
        }
    };

    const handleReset = () => {
        setFiles({
            bankStatement: null,
            bridgeFile: null,
            transactionIds: null
        });
        setResult(null);
        setError(null);
        setProgress(0);
    };

    const handleDownload = async () => {
        try {
            const response = await fetch(`${API_URL}${result.csv_download_url}`);
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `reconciliation_${result.search_id}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            setError('Download failed. Please try again.');
        }
    };

    return (
        <div className="app">
            <header className="header">
                <div className="header-content">
                    <div className="logo">
                        <div className="logo-icon">
                            <svg width="40" height="40" viewBox="0 0 40 40">
                                <circle cx="20" cy="20" r="16" stroke="white" strokeWidth="3" fill="none" />
                                <path d="M14 20L18 24L26 16" stroke="white" strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round" fill="none" />
                            </svg>
                        </div>
                        <div>
                            <h1>UNOTAG</h1>
                            <p>Bank Reconciliation System</p>
                        </div>
                    </div>

                    <div className="header-badges">
                        <div className="badge badge-purple">
                            <svg width="20" height="20" viewBox="0 0 20 20">
                                <path d="M10 2L13 8L19 9L14.5 13.5L15.5 19.5L10 16.5L4.5 19.5L5.5 13.5L1 9L7 8L10 2Z" fill="white" />
                            </svg>
                            <span>100% Accurate</span>
                        </div>
                        <div className="badge badge-blue">
                            <svg width="20" height="20" viewBox="0 0 20 20">
                                <path d="M10 4L13 7M13 7L16 4M13 7V13M19 10C19 14.9706 14.9706 19 10 19C5.02944 19 1 14.9706 1 10C1 5.02944 5.02944 1 10 1" stroke="white" strokeWidth="2" strokeLinecap="round" />
                            </svg>
                            <span>Lightning Fast</span>
                        </div>
                    </div>
                </div>
            </header>

            <main className="main">
                <div className="container">

                    {!result ? (
                        <>
                            <div className="instructions">
                                <h2>Upload Your Files</h2>
                                <p>Drag and drop or click to upload the three required files</p>
                            </div>

                            <div className="upload-grid">
                                <FileUploadCard
                                    title="Bank Statement"
                                    subtitle=".xlsx or .xls file"
                                    file={files.bankStatement}
                                    onChange={(file) => handleFileChange('bankStatement', file)}
                                    step="1"
                                    color="purple"
                                />
                                <FileUploadCard
                                    title="Bridge File"
                                    subtitle=".txt file"
                                    file={files.bridgeFile}
                                    onChange={(file) => handleFileChange('bridgeFile', file)}
                                    step="2"
                                    color="pink"
                                />
                                <FileUploadCard
                                    title="Transaction IDs"
                                    subtitle=".txt or .csv file"
                                    file={files.transactionIds}
                                    onChange={(file) => handleFileChange('transactionIds', file)}
                                    step="3"
                                    color="blue"
                                />
                            </div>

                            {error && (
                                <div className="error-box">
                                    <div className="error-icon">
                                        <svg width="24" height="24" viewBox="0 0 24 24">
                                            <circle cx="12" cy="12" r="10" stroke="white" strokeWidth="2" fill="none" />
                                            <path d="M12 7V13M12 17H12.01" stroke="white" strokeWidth="2.5" strokeLinecap="round" />
                                        </svg>
                                    </div>
                                    <span>{error}</span>
                                </div>
                            )}

                            <button
                                className={`action-button ${processing ? 'processing' : ''} ${!files.bankStatement || !files.bridgeFile || !files.transactionIds ? 'disabled' : ''}`}
                                onClick={handleSubmit}
                                disabled={processing || !files.bankStatement || !files.bridgeFile || !files.transactionIds}
                            >
                                {processing ? (
                                    <>
                                        <div className="spinner"></div>
                                        <span>Processing...</span>
                                    </>
                                ) : (
                                    <>
                                        <svg width="24" height="24" viewBox="0 0 24 24">
                                            <circle cx="12" cy="12" r="10" stroke="white" strokeWidth="2" fill="none" />
                                            <path d="M8 12L11 15L16 9" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                                        </svg>
                                        <span>Start Reconciliation</span>
                                    </>
                                )}
                            </button>

                            {processing && <ProcessingStatus progress={progress} />}
                        </>
                    ) : (
                        <ResultsView result={result} onReset={handleReset} onDownload={handleDownload} />
                    )}

                </div>
            </main>

            <footer className="footer">
                <p>2024 UNOTAG - Built for accuracy and speed</p>
            </footer>
        </div>
    );
}

function FileUploadCard({ title, subtitle, file, onChange, step, color }) {
    const [isDragging, setIsDragging] = useState(false);

    const handleDragOver = (e) => {
        e.preventDefault();
        setIsDragging(true);
    };

    const handleDragLeave = () => {
        setIsDragging(false);
    };

    const handleDrop = (e) => {
        e.preventDefault();
        setIsDragging(false);
        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            onChange(e.dataTransfer.files[0]);
        }
    };

    const handleFileSelect = (e) => {
        if (e.target.files && e.target.files[0]) {
            onChange(e.target.files[0]);
        }
    };

    return (
        <div
            className={`upload-card ${color} ${isDragging ? 'dragging' : ''} ${file ? 'has-file' : ''}`}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
        >
            <div className="step-badge">{step}</div>

            <input
                type="file"
                accept={title.includes('Bank') ? '.xlsx,.xls' : '.txt,.csv'}
                onChange={handleFileSelect}
                id={`file-${step}`}
                style={{ display: 'none' }}
            />

            <label htmlFor={`file-${step}`} className="upload-label">
                <div className="upload-icon-wrapper">
                    <div className="upload-icon">
                        <svg width="50" height="50" viewBox="0 0 50 50">
                            {file ? (
                                <path d="M16 25L21 30L34 17M43 25C43 35.4934 34.4934 44 25 44C15.5066 44 7 35.4934 7 25C7 14.5066 15.5066 6 25 6C34.4934 6 43 14.5066 43 25Z"
                                    stroke="white"
                                    strokeWidth="3.5"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    fill="none"
                                />
                            ) : (
                                <>
                                    <rect x="13" y="18" width="24" height="24" rx="2" stroke="white" strokeWidth="3" strokeDasharray="3 3" fill="none" />
                                    <path d="M25 8L25 28M25 8L21 12M25 8L29 12" stroke="white" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />
                                </>
                            )}
                        </svg>
                    </div>
                </div>

                <h3>{title}</h3>
                <p className="subtitle">{subtitle}</p>

                {file ? (
                    <div className="file-info">
                        <div className="file-icon">
                            <svg width="24" height="24" viewBox="0 0 24 24">
                                <path d="M14 2H6C5.46957 2 4.96086 2.21071 4.58579 2.58579C4.21071 2.96086 4 3.46957 4 4V20C4 20.5304 4.21071 21.0391 4.58579 21.4142C4.96086 21.7893 5.46957 22 6 22H18C18.5304 22 19.0391 21.7893 19.4142 21.4142C19.7893 21.0391 20 20.5304 20 20V8L14 2Z"
                                    stroke="white"
                                    strokeWidth="2"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    fill="none"
                                />
                                <path d="M14 2V8H20" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                            </svg>
                        </div>
                        <div className="file-details">
                            <p className="file-name">{file.name}</p>
                            <p className="file-size">{(file.size / 1024).toFixed(1)} KB</p>
                        </div>
                    </div>
                ) : (
                    <div className="upload-placeholder">
                        <p>Drop file here or click to browse</p>
                    </div>
                )}
            </label>
        </div>
    );
}

function ProcessingStatus({ progress }) {
    const getStatusText = () => {
        if (progress < 30) return 'Reading files...';
        if (progress < 60) return 'Extracting data...';
        if (progress < 90) return 'Matching records...';
        return 'Generating report...';
    };

    const steps = [
        { label: 'Upload', threshold: 25 },
        { label: 'Extract', threshold: 50 },
        { label: 'Match', threshold: 75 },
        { label: 'Report', threshold: 100 }
    ];

    return (
        <div className="processing-status">
            <div className="status-header">
                <div className="pulse-dot"></div>
                <span>{getStatusText()}</span>
            </div>

            <div className="progress-container">
                <div className="progress-bar">
                    <div className="progress-fill" style={{ width: `${progress}%` }}></div>
                </div>
                <span className="progress-text">{progress}%</span>
            </div>

            <div className="processing-steps">
                {steps.map((step, i) => (
                    <React.Fragment key={step.label}>
                        {i > 0 && <div className="step-line"></div>}
                        <div className={`step ${progress >= step.threshold ? 'active' : ''}`}>
                            <div className="step-dot"></div>
                            <span>{step.label}</span>
                        </div>
                    </React.Fragment>
                ))}
            </div>
        </div>
    );
}

function ResultsView({ result, onReset, onDownload }) {
    const matchRate = parseFloat(result.summary.match_rate);

    return (
        <div className="results-container">
            <div className="success-header">
                <div className="success-icon">
                    <svg width="64" height="64" viewBox="0 0 64 64">
                        <circle cx="32" cy="32" r="28" stroke="white" strokeWidth="4" fill="none" />
                        <path d="M20 32L28 40L44 24" stroke="white" strokeWidth="5" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                </div>
                <h2>Reconciliation Complete</h2>
                <p>Processed {result.summary.total_searched.toLocaleString()} records in {result.summary.processing_time}</p>
            </div>

            <div className="match-rate-card">
                <svg width="240" height="240" viewBox="0 0 240 240">
                    <defs>
                        <linearGradient id="progressGradient" x1="0%" y1="0%" x2="100%" y2="100%">
                            <stop offset="0%" stopColor="#667eea" />
                            <stop offset="100%" stopColor="#764ba2" />
                        </linearGradient>
                    </defs>
                    <circle cx="120" cy="120" r="100" fill="none" stroke="#f0f4f8" strokeWidth="16" />
                    <circle
                        cx="120" cy="120" r="100" fill="none" stroke="url(#progressGradient)" strokeWidth="16"
                        strokeDasharray={`${matchRate * 6.28} 628`}
                        strokeLinecap="round"
                        transform="rotate(-90 120 120)"
                    />
                    <text x="120" y="110" textAnchor="middle" dominantBaseline="middle" fontSize="60" fontWeight="900" fill="#2d3748">
                        {matchRate.toFixed(0)}%
                    </text>
                    <text x="120" y="150" textAnchor="middle" dominantBaseline="middle" fontSize="18" fontWeight="700" fill="#718096">
                        Match Rate
                    </text>
                </svg>
            </div>

            <div className="stats-grid">
                <StatCard
                    label="Total Records"
                    value={result.summary.total_searched.toLocaleString()}
                    color="purple"
                />
                <StatCard
                    label="Successfully Matched"
                    value={result.summary.total_found.toLocaleString()}
                    color="pink"
                    highlight
                />
                <StatCard
                    label="Not in Bridge"
                    value={result.summary.not_in_bridge.toLocaleString()}
                    color="blue"
                />
                <StatCard
                    label="Not in Statement"
                    value={result.summary.not_in_statement.toLocaleString()}
                    color="yellow"
                />
            </div>

            <div className="action-buttons">
                <button className="download-button" onClick={onDownload}>
                    <svg width="24" height="24" viewBox="0 0 24 24">
                        <path d="M12 3V15M12 15L8 11M12 15L16 11M3 19H21" stroke="white" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                    <span>Download Full Report</span>
                </button>

                <button className="reset-button" onClick={onReset}>
                    <svg width="24" height="24" viewBox="0 0 24 24">
                        <path d="M3 12C3 7 7 3 12 3C17 3 21 7 21 12C21 17 17 21 12 21M3 12L6 9M3 12L6 15" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                    <span>New Reconciliation</span>
                </button>
            </div>
        </div>
    );
}

function StatCard({ label, value, color, highlight }) {
    return (
        <div className={`stat-card ${color} ${highlight ? 'highlight' : ''}`}>
            <div className="stat-content">
                <p className="stat-label">{label}</p>
                <p className="stat-value">{value}</p>
            </div>
        </div>
    );
}

export default App;