use anyhow::{Context, Result};
use spice_rust_query::{
    CSV_OUTPUT_DIR, PERF_CITY_COUNT, PERF_ROW_COUNT, attacker_ip_for_id, city_id_for_row,
    city_name, nation_name,
};
use std::fs::{File, create_dir_all};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let output_dir = PathBuf::from(CSV_OUTPUT_DIR);

    create_dir_all(&output_dir).with_context(|| {
        format!(
            "failed to create CSV output directory {}",
            output_dir.display()
        )
    })?;

    let city_path = output_dir.join("city_perf.csv");
    let attacker_path = output_dir.join("attacker_ip_perf.csv");

    let mut city_writer = BufWriter::new(
        File::create(&city_path)
            .with_context(|| format!("failed to create {}", city_path.display()))?,
    );
    writeln!(city_writer, "id,city_name,nation")?;
    for id in 1..=PERF_CITY_COUNT {
        writeln!(city_writer, "{},{},{}", id, city_name(id), nation_name(id))?;
    }
    city_writer.flush()?;

    let mut attacker_writer = BufWriter::new(
        File::create(&attacker_path)
            .with_context(|| format!("failed to create {}", attacker_path.display()))?,
    );
    writeln!(attacker_writer, "id,attacker_ip,city_id,city_name")?;
    for id in 1..=PERF_ROW_COUNT {
        let city_id = city_id_for_row(id, PERF_CITY_COUNT);
        writeln!(
            attacker_writer,
            "{},{},{},{}",
            id,
            attacker_ip_for_id(id),
            city_id,
            city_name(city_id)
        )?;
    }
    attacker_writer.flush()?;

    println!(
        "seeded CSV benchmark files: {} and {}",
        attacker_path.display(),
        city_path.display()
    );

    Ok(())
}
