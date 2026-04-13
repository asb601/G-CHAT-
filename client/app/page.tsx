import Link from "next/link";
import { Network, Search, FileStack } from "lucide-react";

export default function HomePage() {
  return (
    <div className="flex flex-col min-h-screen bg-background">
      {/* Navbar */}
      <header className="sticky top-0 z-50 bg-surface/80 backdrop-blur-sm border-b border-border">
        <div className="mx-auto max-w-5xl px-4 h-14 flex items-center justify-between">
          <span className="font-semibold text-foreground text-base">
            Gen-Chatbot
          </span>
          <Link
            href="/login"
            className="inline-flex items-center justify-center h-8 px-4 rounded-md bg-primary text-primary-foreground text-sm font-medium transition-opacity hover:opacity-90"
          >
            Sign In
          </Link>
        </div>
      </header>

      {/* Hero */}
      <main className="flex-1">
        <section className="flex flex-col items-center justify-center text-center min-h-[calc(100vh-3.5rem)] px-4">
          <span className="inline-block mb-5 px-3 py-1 rounded-full bg-primary/10 text-primary text-xs font-medium">
            AI Document Intelligence
          </span>
          <h1 className="text-4xl md:text-6xl font-bold text-foreground leading-tight max-w-2xl">
            Your documents,
            <br />
            finally{" "}
            <span className="text-primary">intelligent</span>
          </h1>
          <p className="mt-5 text-base md:text-lg text-muted-foreground max-w-md">
            Upload files once. Ask anything. Gen-Chatbot builds a knowledge graph
            and answers with context.
          </p>

          {/* Feature cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-16 w-full max-w-3xl">
            <div className="bg-surface border border-border rounded-lg p-5 text-left">
              <Network className="w-5 h-5 text-primary mb-3" />
              <h3 className="text-sm font-semibold text-foreground mb-1">
                Graph RAG
              </h3>
              <p className="text-xs text-muted-foreground">
                Builds a knowledge graph to answer questions with deep context
                and linked reasoning.
              </p>
            </div>
            <div className="bg-surface border border-border rounded-lg p-5 text-left">
              <Search className="w-5 h-5 text-primary mb-3" />
              <h3 className="text-sm font-semibold text-foreground mb-1">
                Hybrid Search
              </h3>
              <p className="text-xs text-muted-foreground">
                Combines vector similarity and keyword search for accurate,
                relevant results every time.
              </p>
            </div>
            <div className="bg-surface border border-border rounded-lg p-5 text-left">
              <FileStack className="w-5 h-5 text-primary mb-3" />
              <h3 className="text-sm font-semibold text-foreground mb-1">
                Multi-format
              </h3>
              <p className="text-xs text-muted-foreground">
                Processes PDF, DOCX, XLSX, CSV, and TXT files from Azure Blob
                Storage seamlessly.
              </p>
            </div>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer className="text-center text-xs text-subtle-foreground mt-16 pb-8">
        Gen-Chatbot &copy; 2025
      </footer>
    </div>
  );
}

