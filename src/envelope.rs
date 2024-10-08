use crate::inscription::{Atom, DeployNFT};
use crate::inscription::{Deploy, DMT};
use log::info;

use {
  super::*,
  bitcoin::blockdata::{
    opcodes,
    script::{self, Instruction, Instructions},
  },
};

pub(crate) const PROTOCOL_ORD_ID: [u8; 3] = *b"ord";
pub(crate) const PROTOCOL_ATOM_ID: [u8; 4] = *b"atom";

pub(crate) const PROTOCOL_ATOM_NFT: [u8; 3] = *b"nft";
pub(crate) const PROTOCOL_ATOM_DFT: [u8; 3] = *b"dft";
pub(crate) const PROTOCOL_ATOM_MOD: [u8; 3] = *b"mod";
pub(crate) const PROTOCOL_ATOM_EVT: [u8; 3] = *b"evt";
pub(crate) const PROTOCOL_ATOM_DMT: [u8; 3] = *b"dmt";
pub(crate) const PROTOCOL_ATOM_DAT: [u8; 3] = *b"dat";

pub(crate) const BODY_TAG: [u8; 0] = [];
pub(crate) const CONTENT_TYPE_TAG: [u8; 1] = [1];
pub(crate) const POINTER_TAG: [u8; 1] = [2];
pub(crate) const PARENT_TAG: [u8; 1] = [3];
pub(crate) const METADATA_TAG: [u8; 1] = [5];
pub(crate) const METAPROTOCOL_TAG: [u8; 1] = [7];

type Result<T> = std::result::Result<T, script::Error>;
type RawEnvelope = Envelope<Vec<Vec<u8>>>;
pub(crate) type ParsedEnvelope = Envelope<Inscription>;
pub(crate) type ParsedAtom = Envelope<Atom>;

#[derive(Debug, Default, PartialEq, Clone)]
pub enum AtomProtocol {
  #[default]
  NFT,
  DFT,
  MOD,
  EVT,
  DMT,
  DAT,
}

#[derive(Debug, Default, PartialEq, Clone)]
pub enum EnvelopeType {
  #[default]
  ORD,
  ATOM(AtomProtocol),
}

#[derive(Debug, Default, PartialEq, Clone)]
pub(crate) struct Envelope<T> {
  pub(crate) payload: T,
  pub(crate) input: u32,
  pub(crate) offset: u32,
  pub(crate) pushnum: bool,
  pub(crate) tx: String,
  pub(crate) e_type: EnvelopeType,
}

fn remove_field(fields: &mut BTreeMap<&[u8], Vec<&[u8]>>, field: &[u8]) -> Option<Vec<u8>> {
  let values = fields.get_mut(field)?;

  if values.is_empty() {
    None
  } else {
    let value = values.remove(0).to_vec();

    if values.is_empty() {
      fields.remove(field);
    }

    Some(value)
  }
}

fn remove_and_concatenate_field(
  fields: &mut BTreeMap<&[u8], Vec<&[u8]>>,
  field: &[u8],
) -> Option<Vec<u8>> {
  let value = fields.remove(field)?;

  if value.is_empty() {
    None
  } else {
    Some(value.into_iter().flatten().cloned().collect())
  }
}
#[derive(Debug)]
pub enum EnvelopeData {
  Brc(ParsedEnvelope),
  Arc(ParsedAtom),
}

impl EnvelopeData {
  pub(crate) fn from_transaction(transaction: &Transaction, tx: String) -> Vec<Self> {
    RawEnvelope::from_transaction(transaction, tx)
      .into_iter()
      .map(|envelope| envelope.into())
      .collect()
  }
}

impl From<RawEnvelope> for EnvelopeData {
  fn from(value: RawEnvelope) -> Self {
    match value.e_type {
      EnvelopeType::ATOM(_) => Self::Arc(value.into()),
      EnvelopeType::ORD => Self::Brc(value.into()),
    }
  }
}

impl From<RawEnvelope> for ParsedEnvelope {
  fn from(envelope: RawEnvelope) -> Self {
    let body = envelope
      .payload
      .iter()
      .enumerate()
      .position(|(i, push)| i % 2 == 0 && push.is_empty());

    let mut fields: BTreeMap<&[u8], Vec<&[u8]>> = BTreeMap::new();

    let mut incomplete_field = false;

    for item in envelope.payload[..body.unwrap_or(envelope.payload.len())].chunks(2) {
      match item {
        [key, value] => fields.entry(key).or_default().push(value),
        _ => incomplete_field = true,
      }
    }

    let duplicate_field = fields.iter().any(|(_key, values)| values.len() > 1);

    let content_type = remove_field(&mut fields, &CONTENT_TYPE_TAG);
    let parent = remove_field(&mut fields, &PARENT_TAG);
    let pointer = remove_field(&mut fields, &POINTER_TAG);
    let metaprotocol = remove_field(&mut fields, &METAPROTOCOL_TAG);
    let metadata = remove_and_concatenate_field(&mut fields, &METADATA_TAG);

    let unrecognized_even_field = fields
      .keys()
      .any(|tag| tag.first().map(|lsb| lsb % 2 == 0).unwrap_or_default());

    Self {
      payload: Inscription {
        body: body.map(|i| {
          envelope.payload[i + 1..]
            .iter()
            .flatten()
            .cloned()
            .collect()
        }),
        content_type,
        parent,
        pointer,
        unrecognized_even_field,
        duplicate_field,
        incomplete_field,
        metaprotocol,
        metadata,
      },
      tx: envelope.tx,
      input: envelope.input,
      offset: envelope.offset,
      pushnum: envelope.pushnum,
      e_type: envelope.e_type,
    }
  }
}

impl ParsedEnvelope {
  pub(crate) fn from_transaction(transaction: &Transaction, tx: String) -> Vec<Self> {
    RawEnvelope::from_transaction(transaction, tx)
      .into_iter()
      .map(|envelope| envelope.into())
      .collect()
  }
}

impl From<RawEnvelope> for ParsedAtom {
  fn from(envelope: RawEnvelope) -> Self {
    let body = envelope
      .payload
      .clone()
      .into_iter()
      .flatten()
      .collect::<Vec<u8>>();
    dbg!(&envelope);
    let payload = match envelope.e_type {
      EnvelopeType::ATOM(ref ty) => match ty {
        AtomProtocol::DMT => Atom::Mint(ciborium::from_reader::<DMT, _>(body.as_slice()).unwrap()),
        AtomProtocol::DFT => {
          Atom::Deploy(ciborium::from_reader::<Deploy, _>(body.as_slice()).unwrap())
        }
        AtomProtocol::NFT => {
          Atom::NFT(ciborium::from_reader::<DeployNFT, _>(body.as_slice()).unwrap())
        }
        _ => {
          todo!()
        }
      },
      _ => unreachable!(),
    };
    Self {
      payload,
      tx: envelope.tx,
      input: envelope.input,
      offset: envelope.offset,
      pushnum: envelope.pushnum,
      e_type: envelope.e_type,
    }
  }
}

impl ParsedAtom {
  pub(crate) fn from_transaction(transaction: &Transaction, tx: String) -> Vec<Self> {
    RawEnvelope::from_transaction(transaction, tx)
      .into_iter()
      .map(|envelope| envelope.into())
      .collect()
  }
}

impl RawEnvelope {
  pub(crate) fn from_transaction(transaction: &Transaction, tx: String) -> Vec<Self> {
    let mut envelopes = Vec::new();

    for (i, input) in transaction.input.iter().enumerate() {
      if let Some(tapscript) = input.witness.tapscript() {
        if let Ok(input_envelopes) = Self::from_tapscript(tapscript, tx.clone(), i) {
          envelopes.extend(input_envelopes);
        }
      }
    }

    envelopes
  }

  fn from_tapscript(tapscript: &Script, tx: String, input: usize) -> Result<Vec<Self>> {
    let mut envelopes = Vec::new();
    let mut instructions = tapscript.instructions();

    while let Some(instruction) = instructions.next() {
      if instruction? == Instruction::PushBytes((&[]).into()) {
        if let Some(envelope) =
          Self::from_instructions(&mut instructions, tx.clone(), input, envelopes.len())?
        {
          envelopes.push(envelope);
        }
      }
    }

    Ok(envelopes)
  }

  fn from_instructions(
    instructions: &mut Instructions,
    tx: String,
    input: usize,
    offset: usize,
  ) -> Result<Option<Self>> {
    if instructions.next().transpose()? != Some(Instruction::Op(opcodes::all::OP_IF)) {
      return Ok(None);
    }

    let check_flag = instructions.next().transpose()?;

    // This is the protocol ord

    if check_flag == Some(Instruction::PushBytes((&PROTOCOL_ORD_ID).into())) {
      let mut pushnum = false;
      let mut payload = Vec::new();

      loop {
        match instructions.next().transpose()? {
          None => return Ok(None),
          Some(Instruction::Op(opcodes::all::OP_ENDIF)) => {
            return Ok(Some(Envelope {
              input: input.try_into().unwrap(),
              offset: offset.try_into().unwrap(),
              payload,
              pushnum,
              tx,
              e_type: EnvelopeType::ORD,
            }));
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_NEG1)) => {
            pushnum = true;
            payload.push(vec![0x81]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_1)) => {
            pushnum = true;
            payload.push(vec![1]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_2)) => {
            pushnum = true;
            payload.push(vec![2]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_3)) => {
            pushnum = true;
            payload.push(vec![3]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_4)) => {
            pushnum = true;
            payload.push(vec![4]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_5)) => {
            pushnum = true;
            payload.push(vec![5]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_6)) => {
            pushnum = true;
            payload.push(vec![6]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_7)) => {
            pushnum = true;
            payload.push(vec![7]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_8)) => {
            pushnum = true;
            payload.push(vec![8]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_9)) => {
            pushnum = true;
            payload.push(vec![9]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_10)) => {
            pushnum = true;
            payload.push(vec![10]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_11)) => {
            pushnum = true;
            payload.push(vec![11]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_12)) => {
            pushnum = true;
            payload.push(vec![12]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_13)) => {
            pushnum = true;
            payload.push(vec![13]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_14)) => {
            pushnum = true;
            payload.push(vec![14]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_15)) => {
            pushnum = true;
            payload.push(vec![15]);
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_16)) => {
            pushnum = true;
            payload.push(vec![16]);
          }
          Some(Instruction::PushBytes(push)) => {
            payload.push(push.as_bytes().to_vec());
          }
          Some(_) => return Ok(None),
        }
      }
    }

    if check_flag == Some(Instruction::PushBytes((&PROTOCOL_ATOM_ID).into())) {
      let mut payload = Vec::new();
      let mut pushnum = false;
      let mut e_type = EnvelopeType::ATOM(AtomProtocol::default());

      loop {
        let op_code = instructions.next().transpose()?;

        match op_code {
          None => return Ok(None),
          Some(Instruction::Op(opcodes::all::OP_ENDIF)) => {
            return Ok(Some(Envelope {
              input: input.try_into().unwrap(),
              offset: offset.try_into().unwrap(),
              payload,
              pushnum,
              tx,
              e_type,
            }));
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_NEG1)) => {
            info!("Got Atom Protocol Pushnum Neg1");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_1)) => {
            info!("Got Atom Protocol Pushnum 1");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_2)) => {
            info!("Got Atom Protocol Pushnum 2");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_3)) => {
            info!("Got Atom Protocol Pushnum 3");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_4)) => {
            info!("Got Atom Protocol Pushnum 4");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_5)) => {
            info!("Got Atom Protocol Pushnum 5");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_6)) => {
            info!("Got Atom Protocol Pushnum 6");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_7)) => {
            info!("Got Atom Protocol Pushnum 7");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_8)) => {
            info!("Got Atom Protocol Pushnum 8");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_9)) => {
            info!("Got Atom Protocol Pushnum 9");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_10)) => {
            info!("Got Atom Protocol Pushnum 10");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_11)) => {
            info!("Got Atom Protocol Pushnum 11");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_12)) => {
            info!("Got Atom Protocol Pushnum 12");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_13)) => {
            info!("Got Atom Protocol Pushnum 13");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_14)) => {
            info!("Got Atom Protocol Pushnum 14");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_15)) => {
            info!("Got Atom Protocol Pushnum 15");
          }
          Some(Instruction::Op(opcodes::all::OP_PUSHNUM_16)) => {
            info!("Got Atom Protocol Pushnum 16");
          }
          Some(Instruction::PushBytes(push)) => {
            let three_letter_op_len = 3;
            if push.len() == three_letter_op_len {
              let atom_op = push.as_bytes();
              if atom_op == PROTOCOL_ATOM_NFT {
                dbg!("Got Atom Protocol NFT");
                e_type = EnvelopeType::ATOM(AtomProtocol::NFT);
                return Ok(None);
              } else if atom_op == PROTOCOL_ATOM_DFT {
                // Deploy FT
                dbg!("Got Atom Protocol DFT");
                e_type = EnvelopeType::ATOM(AtomProtocol::DFT);
              } else if atom_op == PROTOCOL_ATOM_MOD {
                dbg!("Got Atom Protocol MOD");
                e_type = EnvelopeType::ATOM(AtomProtocol::MOD);
                return Ok(None);
              } else if atom_op == PROTOCOL_ATOM_EVT {
                dbg!("Got Atom Protocol EVT");
                e_type = EnvelopeType::ATOM(AtomProtocol::EVT);
                return Ok(None);
              } else if atom_op == PROTOCOL_ATOM_DMT {
                e_type = EnvelopeType::ATOM(AtomProtocol::DMT);
                dbg!("Got Atom Protocol DMT");
              } else if atom_op == PROTOCOL_ATOM_DAT {
                e_type = EnvelopeType::ATOM(AtomProtocol::DAT);
                dbg!("Got Atom Protocol DAT");
                return Ok(None);
              } else {
                return Ok(None);
              }
            } else {
              pushnum = true;
              payload.push(push.as_bytes().to_vec());
            }
          }
          Some(_) => return Ok(None),
        }
      }
    }

    Ok(None)
  }
}

#[cfg(test)]
mod tests {
  use {super::*, bitcoin::absolute::LockTime};

  fn parse(witnesses: &[Witness]) -> Vec<ParsedEnvelope> {
    ParsedEnvelope::from_transaction(&Transaction {
      version: 0,
      lock_time: LockTime::ZERO,
      input: witnesses
        .iter()
        .map(|witness| TxIn {
          previous_output: OutPoint::null(),
          script_sig: ScriptBuf::new(),
          sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
          witness: witness.clone(),
        })
        .collect(),
      output: Vec::new(),
    })
  }

  #[test]
  fn empty() {
    assert_eq!(parse(&[Witness::new()]), Vec::new())
  }

  #[test]
  fn ignore_key_path_spends() {
    assert_eq!(
      parse(&[Witness::from_slice(&[bitcoin::script::Builder::new()
        .push_opcode(bitcoin::opcodes::OP_FALSE)
        .push_opcode(bitcoin::opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_opcode(bitcoin::opcodes::all::OP_ENDIF)
        .into_script()
        .into_bytes()])]),
      Vec::new()
    );
  }

  #[test]
  fn ignore_key_path_spends_with_annex() {
    assert_eq!(
      parse(&[Witness::from_slice(&[
        bitcoin::script::Builder::new()
          .push_opcode(bitcoin::opcodes::OP_FALSE)
          .push_opcode(bitcoin::opcodes::all::OP_IF)
          .push_slice(b"ord")
          .push_opcode(bitcoin::opcodes::all::OP_ENDIF)
          .into_script()
          .into_bytes(),
        vec![0x50]
      ])]),
      Vec::new()
    );
  }

  #[test]
  fn parse_from_tapscript() {
    assert_eq!(
      parse(&[Witness::from_slice(&[
        bitcoin::script::Builder::new()
          .push_opcode(bitcoin::opcodes::OP_FALSE)
          .push_opcode(bitcoin::opcodes::all::OP_IF)
          .push_slice(b"ord")
          .push_opcode(bitcoin::opcodes::all::OP_ENDIF)
          .into_script()
          .into_bytes(),
        Vec::new()
      ])]),
      vec![ParsedEnvelope {
        ..Default::default()
      }]
    );
  }

  #[test]
  fn ignore_unparsable_scripts() {
    let mut script_bytes = bitcoin::script::Builder::new()
      .push_opcode(bitcoin::opcodes::OP_FALSE)
      .push_opcode(bitcoin::opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_opcode(bitcoin::opcodes::all::OP_ENDIF)
      .into_script()
      .into_bytes();
    script_bytes.push(0x01);

    assert_eq!(
      parse(&[Witness::from_slice(&[script_bytes, Vec::new()])]),
      Vec::new()
    );
  }

  #[test]
  fn no_inscription() {
    assert_eq!(
      parse(&[Witness::from_slice(&[
        ScriptBuf::new().into_bytes(),
        Vec::new()
      ])]),
      Vec::new()
    );
  }

  #[test]
  fn duplicate_field() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[255], &[], &[255], &[]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          duplicate_field: true,
          ..Default::default()
        },
        ..Default::default()
      }]
    );
  }

  #[test]
  fn with_content_type() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"ord",
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "ord"),
        ..Default::default()
      }]
    );
  }

  #[test]
  fn with_unknown_tag() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[9],
        b"bar",
        &[],
        b"ord",
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "ord"),
        ..Default::default()
      }]
    );
  }

  #[test]
  fn no_body() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[1], b"text/plain;charset=utf-8"])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          content_type: Some(b"text/plain;charset=utf-8".to_vec()),
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn no_content_type() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[], b"foo"])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          body: Some(b"foo".to_vec()),
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn valid_body_in_multiple_pushes() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"foo",
        b"bar"
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "foobar"),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn valid_body_in_zero_pushes() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[1], b"text/plain;charset=utf-8", &[]])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", ""),
        ..Default::default()
      }]
    );
  }

  #[test]
  fn valid_body_in_multiple_empty_pushes() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        &[],
        &[],
        &[],
        &[],
        &[],
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", ""),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn valid_ignore_trailing() {
    let script = script::Builder::new()
      .push_opcode(opcodes::OP_FALSE)
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_slice([1])
      .push_slice(b"text/plain;charset=utf-8")
      .push_slice([])
      .push_slice(b"ord")
      .push_opcode(opcodes::all::OP_ENDIF)
      .push_opcode(opcodes::all::OP_CHECKSIG)
      .into_script();

    assert_eq!(
      parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "ord"),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn valid_ignore_preceding() {
    let script = script::Builder::new()
      .push_opcode(opcodes::all::OP_CHECKSIG)
      .push_opcode(opcodes::OP_FALSE)
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_slice([1])
      .push_slice(b"text/plain;charset=utf-8")
      .push_slice([])
      .push_slice(b"ord")
      .push_opcode(opcodes::all::OP_ENDIF)
      .into_script();

    assert_eq!(
      parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "ord"),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn multiple_inscriptions_in_a_single_witness() {
    let script = script::Builder::new()
      .push_opcode(opcodes::OP_FALSE)
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_slice([1])
      .push_slice(b"text/plain;charset=utf-8")
      .push_slice([])
      .push_slice(b"foo")
      .push_opcode(opcodes::all::OP_ENDIF)
      .push_opcode(opcodes::OP_FALSE)
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_slice([1])
      .push_slice(b"text/plain;charset=utf-8")
      .push_slice([])
      .push_slice(b"bar")
      .push_opcode(opcodes::all::OP_ENDIF)
      .into_script();

    assert_eq!(
      parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
      vec![
        ParsedEnvelope {
          payload: inscription("text/plain;charset=utf-8", "foo"),
          ..Default::default()
        },
        ParsedEnvelope {
          payload: inscription("text/plain;charset=utf-8", "bar"),
          offset: 1,
          ..Default::default()
        },
      ],
    );
  }

  #[test]
  fn invalid_utf8_does_not_render_inscription_invalid() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        &[0b10000000]
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", [0b10000000]),
        ..Default::default()
      },],
    );
  }

  #[test]
  fn no_endif() {
    let script = script::Builder::new()
      .push_opcode(opcodes::OP_FALSE)
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .into_script();

    assert_eq!(
      parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
      Vec::new(),
    );
  }

  #[test]
  fn no_op_false() {
    let script = script::Builder::new()
      .push_opcode(opcodes::all::OP_IF)
      .push_slice(b"ord")
      .push_opcode(opcodes::all::OP_ENDIF)
      .into_script();

    assert_eq!(
      parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
      Vec::new(),
    );
  }

  #[test]
  fn empty_envelope() {
    assert_eq!(parse(&[envelope(&[])]), Vec::new());
  }

  #[test]
  fn wrong_protocol_identifier() {
    assert_eq!(parse(&[envelope(&[b"foo"])]), Vec::new());
  }

  #[test]
  fn extract_from_transaction() {
    assert_eq!(
      parse(&[envelope(&[
        b"ord",
        &[1],
        b"text/plain;charset=utf-8",
        &[],
        b"ord"
      ])]),
      vec![ParsedEnvelope {
        payload: inscription("text/plain;charset=utf-8", "ord"),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn extract_from_second_input() {
    assert_eq!(
      parse(&[Witness::new(), inscription("foo", [1; 1040]).to_witness()]),
      vec![ParsedEnvelope {
        payload: inscription("foo", [1; 1040]),
        input: 1,
        ..Default::default()
      }]
    );
  }

  #[test]
  fn extract_from_second_envelope() {
    let mut builder = script::Builder::new();
    builder = inscription("foo", [1; 100]).append_reveal_script_to_builder(builder);
    builder = inscription("bar", [1; 100]).append_reveal_script_to_builder(builder);

    assert_eq!(
      parse(&[Witness::from_slice(&[
        builder.into_script().into_bytes(),
        Vec::new()
      ])]),
      vec![
        ParsedEnvelope {
          payload: inscription("foo", [1; 100]),
          ..Default::default()
        },
        ParsedEnvelope {
          payload: inscription("bar", [1; 100]),
          offset: 1,
          ..Default::default()
        }
      ]
    );
  }

  #[test]
  fn inscribe_png() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[1], b"image/png", &[], &[1; 100]])]),
      vec![ParsedEnvelope {
        payload: inscription("image/png", [1; 100]),
        ..Default::default()
      }]
    );
  }

  #[test]
  fn chunked_data_is_parsable() {
    let mut witness = Witness::new();

    witness.push(&inscription("foo", [1; 1040]).append_reveal_script(script::Builder::new()));

    witness.push([]);

    assert_eq!(
      parse(&[witness]),
      vec![ParsedEnvelope {
        payload: inscription("foo", [1; 1040]),
        ..Default::default()
      }]
    );
  }

  #[test]
  fn round_trip_with_no_fields() {
    let mut witness = Witness::new();

    witness.push(Inscription::default().append_reveal_script(script::Builder::new()));

    witness.push([]);

    assert_eq!(
      parse(&[witness]),
      vec![ParsedEnvelope {
        payload: Inscription::default(),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn unknown_odd_fields_are_ignored() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[9], &[0]])]),
      vec![ParsedEnvelope {
        payload: Inscription::default(),
        ..Default::default()
      }],
    );
  }

  #[test]
  fn unknown_even_fields() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[22], &[0]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          unrecognized_even_field: true,
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn pointer_field_is_recognized() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[2], &[1]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          pointer: Some(vec![1]),
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn duplicate_pointer_field_makes_inscription_unbound() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[2], &[1], &[2], &[0]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          pointer: Some(vec![1]),
          duplicate_field: true,
          unrecognized_even_field: true,
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn incomplete_field() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[99]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          incomplete_field: true,
          ..Default::default()
        },
        ..Default::default()
      }],
    );
  }

  #[test]
  fn metadata_is_parsed_correctly() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[5], &[]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          metadata: Some(vec![]),
          ..Default::default()
        },
        ..Default::default()
      }]
    );
  }

  #[test]
  fn metadata_is_parsed_correctly_from_chunks() {
    assert_eq!(
      parse(&[envelope(&[b"ord", &[5], &[0], &[5], &[1]])]),
      vec![ParsedEnvelope {
        payload: Inscription {
          metadata: Some(vec![0, 1]),
          duplicate_field: true,
          ..Default::default()
        },
        ..Default::default()
      }]
    );
  }

  #[test]
  fn pushnum_opcodes_are_parsed_correctly() {
    const PUSHNUMS: &[(opcodes::All, u8)] = &[
      (opcodes::all::OP_PUSHNUM_NEG1, 0x81),
      (opcodes::all::OP_PUSHNUM_1, 1),
      (opcodes::all::OP_PUSHNUM_2, 2),
      (opcodes::all::OP_PUSHNUM_3, 3),
      (opcodes::all::OP_PUSHNUM_4, 4),
      (opcodes::all::OP_PUSHNUM_5, 5),
      (opcodes::all::OP_PUSHNUM_6, 6),
      (opcodes::all::OP_PUSHNUM_7, 7),
      (opcodes::all::OP_PUSHNUM_8, 8),
      (opcodes::all::OP_PUSHNUM_9, 9),
      (opcodes::all::OP_PUSHNUM_10, 10),
      (opcodes::all::OP_PUSHNUM_11, 11),
      (opcodes::all::OP_PUSHNUM_12, 12),
      (opcodes::all::OP_PUSHNUM_13, 13),
      (opcodes::all::OP_PUSHNUM_14, 14),
      (opcodes::all::OP_PUSHNUM_15, 15),
      (opcodes::all::OP_PUSHNUM_16, 16),
    ];

    for &(op, value) in PUSHNUMS {
      let script = script::Builder::new()
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(opcodes::all::OP_IF)
        .push_slice(b"ord")
        .push_opcode(opcodes::OP_FALSE)
        .push_opcode(op)
        .push_opcode(opcodes::all::OP_ENDIF)
        .into_script();

      assert_eq!(
        parse(&[Witness::from_slice(&[script.into_bytes(), Vec::new()])]),
        vec![ParsedEnvelope {
          payload: Inscription {
            body: Some(vec![value]),
            ..Default::default()
          },
          pushnum: true,
          ..Default::default()
        }],
      );
    }
  }
}
