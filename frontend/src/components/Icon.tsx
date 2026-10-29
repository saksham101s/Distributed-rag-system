const PATHS = {
  chat: "M4 5h16v11H8l-4 4V5z",
  docs: "M6 3h8l4 4v14H6V3zm8 0v4h4M9 12h6M9 16h6",
  cluster: "M12 3a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5zM5 15a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5zm14 0a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5zM10.6 7.6 6.4 15.2m7-7.6 4.2 7.6M7.5 17.5h9",
  send: "M4 12 20 4l-6 16-2.5-6.5L4 12z",
  stop: "M7 7h10v10H7z",
  upload: "M12 16V4m0 0L7 9m5-5 5 5M4 16v4h16v-4",
  trash: "M5 7h14M10 7V4h4v3m-7 0 1 13h8l1-13",
  eye: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zm10-3a3 3 0 1 1 0 6 3 3 0 0 1 0-6z",
  close: "M6 6l12 12M18 6 6 18",
  repair: "M14.7 6.3a4 4 0 0 0-5.4 5.4L4 17l3 3 5.3-5.3a4 4 0 0 0 5.4-5.4l-2.5 2.5-2.5-.5-.5-2.5 2.5-2.5z",
  bolt: "M13 2 4 14h7l-1 8 9-12h-7l1-8z",
  pause: "M8 5h3v14H8zm5 0h3v14h-3z",
  play: "M7 4v16l13-8L7 4z",
  wipe: "M3 20h18M7 16 17 6l3 3-10 10H7v-3z",
  down: "M12 3a9 9 0 1 1 0 18 9 9 0 0 1 0-18zM8 8l8 8",
  up: "M12 3a9 9 0 1 1 0 18 9 9 0 0 1 0-18zm-4 9 3 3 5-6",
  plus: "M12 5v14M5 12h14",
  copy: "M8 8h11v11H8zM5 16V5h11",
} as const;

export type IconName = keyof typeof PATHS;

export function Icon({ name, size = 16, title }: { name: IconName; size?: number; title?: string }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.8}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden={title ? undefined : true}
      role={title ? "img" : undefined}
      className="icon"
    >
      {title && <title>{title}</title>}
      <path d={PATHS[name]} />
    </svg>
  );
}
