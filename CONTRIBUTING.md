# Contributing to Sif

## Rules

- We love pull requests but, given the early stage of the project, we love discussions too! If you have an idea, open an issue and pitch your next cool feature :)
- Use feature branches. **Create an issue for every single feature or bug** and **label** it. If you are a core contributor, create a branch named feature/[issue #] to resolve the issue. If you are not a core contributor, fork and branch.
- Try to label issues and PRs as accurately as possible, especially in the case of PRs, where `semver-major`, `semver-minor` and `semver-patch` labels are available. Labels on issues should be a subset of the labels on the corresponding pull request; the milestones should match.
- Use github "Fixes #[issue]" syntax on your PRs to indicate which issues you are attempting to resolve
- Keep Sif small. If some piece of functionality can fit in a separate `sif-*` module, then it probably should

## Sign-off your Commits

We encourage contributors to "sign-off" on their commits:

```bash
$ git commit -s -m "Some commit message"
```

This will append the following to your commit message:

```
Signed-off-by: Your Name <your@email.com>
```

This sign-off certifies that you have the rights to submit your work under the [Apache 2.0 license](https://opensource.org/licenses/Apache-2.0) and that you agree to the [Developer Certificate of Origin](http://developercertificate.org/):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

### Quick Tip

If you wish to sign-off all commits against this repository automatically, use this command:

```bash
$ git config alias.cs 'commit -s'
```

## Developing

Development environments should employ go 1.13+, with module support enabled.

### Getting Started

Install the protobuf `protoc` compiler from here: https://github.com/google/protobuf/releases

Then,

```bash
make dependencies
```

### Running the Tests

```bash
make test
# or, to include long tests:
make testall
```
