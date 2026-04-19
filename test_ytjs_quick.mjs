import Innertube from "npm:youtubei.js@14.0.0";

const vids = ["JGwWNGJdvx8", "aJOTlE1K90k", "vGJTaP6anOU"];
const yt = await Innertube.create();

for (const vid of vids) {
  try {
    const start = Date.now();
    const info = await yt.getInfo(vid);
    const sd = info.streaming_data;
    const adaptive = sd?.adaptive_formats || [];
    const audio = adaptive.filter((f) => f.mime_type?.startsWith("audio/"));
    const hasSabr = !!sd?.server_abr_streaming_url;
    const hasAudioUrl = audio.some((f) => !!f.url);
    const title = info.basic_info?.title || "?";
    console.log(`${vid}: audio=${audio.length} | has_url=${hasAudioUrl} | sabr=${hasSabr} | ${title.substring(0, 50)} | ${Date.now() - start}ms`);
  } catch (e) {
    console.log(`${vid}: FAILED | ${e.message.substring(0, 80)}`);
  }
}
