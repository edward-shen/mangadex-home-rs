create table if not exists Images(
    id varchar primary key not null,
    size integer not null,
    accessed timestamp not null default CURRENT_TIMESTAMP,
    disk_size integer as ((size + 4095) / 4096 * 4096)
);
create index if not exists Images_accessed on Images(accessed);