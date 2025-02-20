# Persevere &ndash; a tool for reliably uploading huge files to S3

With Persevere you can upload huge files to S3 without worrying about network interruptions or other issues.
Persevere will allow you to resume the upload where it was left off, even in the case of a system crash during upload.

The contents of the file you upload are always streamed, which means the memory usage of Persevere is minimal, usually below 10 MB.
This makes it possible to upload files of any size supported by S3, even if they are larger than the available memory of your system.

> [!IMPORTANT]
> This project is still in fairly early development.
> Although we have used it to upload files up to 3 TB in size reliably, there is a chance that there are bugs that could lead to corrupt objects in S3.
> 
> For files where it is vital to you that the object that ends up in S3 is valid, consider one of these options:
>
> * Download the object again and verify its checksum versus the original locally.
> * Let S3 calculate a checksum after the object is uploaded through e.g. the AWS Console.
>
>   (Please note that when S3 calculates the checksum it will copy the object onto itself, which might incur additional costs.)
> 
> We are planning on adding automatic checksum calculation on upload, as well as per-part checksums, which takes this burden off of you.

## Installation

Currently, there are no pre-built binaries available.
Installation of Persevere requires checking out this repository and building it yourself.
You need to have [Rust](https://www.rust-lang.org) installed on your system.

```sh
$ git clone https://github.com/takkt-ag/persevere.git
$ cd persevere
$ cargo build --release
```

This will create the binary in:

* `target/release/persevere` on Unix-like systems
* `target\release\persevere.exe` on Windows

## Usage

Persevere is a command-line tool, so interactions with it happen from a terminal.
A normal workflow of using Persevere means invoking the `upload` command for the file you want to upload.

Assume you have a very large file called `database.dump` that you want to upload to the S3 bucket `my-bucket` under the key `backups/database.dump`.
You can use Persevere as such to upload this file:

```sh
persevere upload start --s3-bucket my-bucket --s3-key backups/database.dump --file-to-upload database.dump --state-file database.dump.persevere-state
```

The actual name of the state-file does not matter, just make it something that makes sense to you!
Once you execute the command, the upload will start immediately, showing you the status of the upload as it progresses.

If the upload is interrupted for any reason, you can resume it by running the `resume` command, providing the same state-file again:

```sh
persevere upload resume --state-file database.dump.persevere-state
```

Should you, for any reason, want to abort the upload before it has finished, you can do so by running the `abort` command, again providing the same state-file:

```sh
persevere upload abort --state-file database.dump.persevere-state
```

To see all available commands, run:

```sh
persevere --help
```

If you want to see the help for a specific command, run:

```sh
persevere <command> --help
```

## AWS credentials and permissions

An upload to S3 obviously requires some credentials and permissions to work.

Persevere will automatically discover valid AWS credentials like most AWS SDKs.
This means you can provide environment variables such as `AWS_PROFILE` to select the profile you want to upload a file with, or provide the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` directly.

If you are running Persevere on an AWS resource that has an AWS role attached (like the instance profile of an EC2 instance, or the task-role of an ECS task), Persevere will automatically use the credentials of that role.

Regardless of how the credentials are provided, the user or role must have the necessary permissions to upload to the S3 bucket and key you specify.
Only the `s3:PutObject` and `s3:AbortMultipartUpload` actions need to be allowed.

A valid IAM policy can look like this:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:AbortMultipartUpload"
            ],
            "Resource": "arn:aws:s3:::my-bucket/backups/*"
        }
    ]
}
```

## Comparison to other tools

There are many tools available that allow you to upload files to S3, although we have found none that:

* Deal well with interruptions during the upload.
* Don't require a language runtime (like Python or Node.js) to be installed.

Persevere is trying to fill specifically this gap, which means it is not a replacement for the various other tools, but rather an addition.

If you are looking for other features, such as:

* Downloads and uploads highly optimized for speed.
* Downloads and uploads of many files at once.
* Synchronization of files between local and S3.
* Management of S3 buckets and objects.

You might want to look at other tools, such as:

* The official [AWS CLI](https://aws.amazon.com/cli/).
* s3cmd: <https://github.com/s3tools/s3cmd>
* s4cmd: <https://github.com/bloomreach/s4cmd>
* s5cmd: <https://github.com/peak/s5cmd>

<sub>
    (We do not explicitly endorse the use of these tools, they are just examples of tools that are available that might fit your needs better.
    Make sure to evaluate them yourself to see if they fit your use-case.)
</sub>

## Planned features

Persevere is not intended to become a full-featured S3 client: it is meant to be a tool that allows you to upload huge files to S3, **reliably**.

Still, there are some features that we believe are necessary to make Persevere a complete tool for this purpose:

* Automatic checksum calculation on upload.
* Per-part checksums.

Additionally, we think there might be features that could be useful to many users, enhancing the applicability of Persevere, without bloating it:

* Uploading multiple parts in parallel to speed up uploads.

If you are interested in contributing a feature that is not mentioned here, we suggest to reach out through an issue first to see if the feature is something we would like to see in Persevere.

## License

Persevere is licensed under the Apache License, Version 2.0, (see [LICENSE](LICENSE) or <https://www.apache.org/licenses/LICENSE-2.0>).

Persevere internally makes use of various open-source projects.
You can find a full list of these projects and their licenses in [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Persevere by you, as defined in the Apache-2.0 license, shall be licensed under the Apache License, Version 2.0, without any additional terms or conditions.

We require code submitted to be formatted with Rust's default rustfmt formatter (CI will automatically verified if your code is formatted correctly).
We are using unstable rustfmt formatting rules, which requires running the formatter with a nightly toolchain, which you can do as follows:

```sh
$ rustup toolchain install nightly
$ cargo +nightly fmt
```

(Building and running Persevere itself can and should happen with the stable toolchain.)

Additionally we are also checking whether there are any clippy warnings in your code.
You can run clippy locally with:

```sh
$ cargo clippy --workspace --lib --bins --tests --all-targets -- -Dwarnings
```

There can be occasions where newer versions of clippy warn about code you haven't touched.
In such cases we'll try to get those warnings resolved before merging your changes, or work together with you to get them resolved in your merge request.

## Affiliation

This project has no official affiliation with Amazon Web Services, Inc., Amazon.com, Inc., or any of its affiliates.
"Amazon Web Services", "Amazon Simple Storage Service" and "Amazon S3" are trademarks of Amazon.com, Inc. or its affiliates in the United States and/or other countries.
