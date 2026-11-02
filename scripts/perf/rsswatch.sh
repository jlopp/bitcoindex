#!/usr/bin/env bash
# Sample RSS + anon/file-backed split of a bidx process every 2s.
# Usage: rsswatch.sh <pid> <logfile>
pid=$1; log=$2
: > "$log"
peak=0
while kill -0 "$pid" 2>/dev/null; do
  rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null)
  anon=$(awk '/RssAnon/{print $2}' /proc/$pid/status 2>/dev/null)
  file=$(awk '/RssFile/{print $2}' /proc/$pid/status 2>/dev/null)
  [ -z "$rss" ] && break
  printf '%s rss_mb=%.0f anon_mb=%.0f file_mb=%.0f\n' \
    "$(date +%H:%M:%S)" "$(echo "$rss/1024"|bc -l)" "$(echo "$anon/1024"|bc -l)" \
    "$(echo "$file/1024"|bc -l)" >> "$log"
  if [ "$rss" -gt "$peak" ]; then peak=$rss; fi
  sleep 2
done
echo "PEAK_RSS_MB=$((peak/1024))" >> "$log"
