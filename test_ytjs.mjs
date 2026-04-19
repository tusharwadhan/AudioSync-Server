/**
 * Standalone YouTube.js + googlevideo SABR test.
 * Run with Deno on Render to test if native SABR protocol works from datacenter IP.
 *
 * Usage:
 *   deno run --allow-net --allow-read --allow-write --allow-env test_ytjs.mjs
 *
 * Or serve as HTTP:
 *   deno run --allow-net --allow-read --allow-write --allow-env test_ytjs.mjs --serve
 */

import Innertube from "https://esm.sh/youtubei.js@14.0.0/web.bundle.min";

const TEST_VIDEOS = [
  "JGwWNGJdvx8", // Ed Sheeran
  "aJOTlE1K90k", // AP Dhillon
  "vGJTaP6anOU", // Arijit Singh
  "lp-EO5I60KA", // Sidhu Moose Wala
  "dQw4w9WgXcQ", // Rick Astley
];

async function testVideo(videoId) {
  const start = Date.now();
  try {
    const innertube = await Innertube.create();
    const info = await innertube.getInfo(videoId);

    // Check streaming data
    const streamingData = info.streaming_data;

    if (!streamingData) {
      return {
        video_id: videoId,
        success: false,
        error: "No streaming data returned",
        time_ms: Date.now() - start,
      };
    }

    // Check for adaptive formats (audio-only)
    const adaptiveFormats = streamingData.adaptive_formats || [];
    const audioFormats = adaptiveFormats.filter(
      (f) => f.mime_type && f.mime_type.startsWith("audio/")
    );

    // Check for regular formats
    const formats = streamingData.formats || [];

    // Check for SABR server streaming
    const hasSabr = !!streamingData.server_abr_streaming_url;

    // Try to get an audio URL
    let audioUrl = null;
    let audioFormat = null;

    if (audioFormats.length > 0) {
      const best = audioFormats[audioFormats.length - 1];
      audioUrl = best.url || best.decipher?.(innertube.session.player);
      audioFormat = {
        mime: best.mime_type,
        bitrate: best.bitrate,
        quality: best.audio_quality,
      };
    } else if (formats.length > 0) {
      // Fallback to muxed format
      const best = formats[formats.length - 1];
      audioUrl = best.url || best.decipher?.(innertube.session.player);
      audioFormat = {
        mime: best.mime_type,
        bitrate: best.bitrate,
        quality: best.quality_label,
      };
    }

    return {
      video_id: videoId,
      success: !!audioUrl,
      title: info.basic_info?.title,
      has_sabr: hasSabr,
      sabr_url_preview: streamingData.server_abr_streaming_url
        ? streamingData.server_abr_streaming_url.substring(0, 100) + "..."
        : null,
      audio_format: audioFormat,
      audio_url_preview: audioUrl ? audioUrl.substring(0, 100) + "..." : null,
      total_adaptive_formats: adaptiveFormats.length,
      audio_formats_count: audioFormats.length,
      muxed_formats_count: formats.length,
      format_details: adaptiveFormats.slice(0, 10).map((f) => ({
        mime: f.mime_type,
        bitrate: f.bitrate,
        has_url: !!f.url,
        quality: f.audio_quality || f.quality_label,
      })),
      time_ms: Date.now() - start,
    };
  } catch (e) {
    return {
      video_id: videoId,
      success: false,
      error: e.message,
      time_ms: Date.now() - start,
    };
  }
}

// Check if running as HTTP server
const isServe = Deno.args.includes("--serve");

if (isServe) {
  const port = parseInt(Deno.env.get("PORT") || "3000");
  console.log(`YouTube.js test server running on port ${port}`);

  Deno.serve({ port }, async (req) => {
    const url = new URL(req.url);

    if (url.pathname === "/health") {
      return Response.json({ status: "ok", engine: "youtube.js" });
    }

    if (url.pathname === "/") {
      return Response.json({
        message: "YouTube.js + googlevideo SABR test server",
        endpoints: {
          "/test/<videoId>": "Test one video",
          "/test-all": "Test all sample videos",
          "/health": "Health check",
        },
      });
    }

    if (url.pathname.startsWith("/test/")) {
      const videoId = url.pathname.split("/test/")[1];
      if (videoId && videoId.length >= 11) {
        const result = await testVideo(videoId);
        return Response.json(result);
      }
      return Response.json({ error: "Invalid video ID" }, { status: 400 });
    }

    if (url.pathname === "/test-all") {
      const results = {};
      for (const vid of TEST_VIDEOS) {
        results[vid] = await testVideo(vid);
      }
      return Response.json({
        any_success: Object.values(results).some((r) => r.success),
        results,
      });
    }

    return Response.json({ error: "Not found" }, { status: 404 });
  });
} else {
  // CLI mode - test all videos and print results
  console.log("Testing YouTube.js extraction from this IP...\n");

  for (const vid of TEST_VIDEOS) {
    console.log(`Testing ${vid}...`);
    const result = await testVideo(vid);
    console.log(
      `  Success: ${result.success} | SABR: ${result.has_sabr} | Audio formats: ${result.audio_formats_count} | Time: ${result.time_ms}ms`
    );
    if (result.success) {
      console.log(`  Title: ${result.title}`);
      console.log(`  Audio: ${JSON.stringify(result.audio_format)}`);
    } else {
      console.log(`  Error: ${result.error || "No audio URL found"}`);
    }
    console.log();
  }
}
