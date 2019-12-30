/// Conditional compilation depending on whether Serde is built with support for
/// 128-bit integers.
///
/// Data formats that wish to support Rust compiler versions older than 1.26 may
/// place the i128 / u128 methods of their Serializer and Deserializer behind
/// this macro.
///
/// Data formats that require a minimum Rust compiler version of at least 1.26
/// do not need to bother with this macro and may assume support for 128-bit
/// integers.
///
/// ```rust
/// #[macro_use]
/// extern crate serde;
///
/// use serde::Serializer;
/// # use serde::private::ser::Error;
/// #
/// # struct MySerializer;
///
/// impl Serializer for MySerializer {
///     type Ok = ();
///     type Error = Error;
///
///     fn se_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
///         /* ... */
/// #         unimplemented!()
///     }
///
///     /* ... */
///
///     serde_if_integer128! {
///         fn se_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
///             /* ... */
/// #             unimplemented!()
///         }
///
///         fn se_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
///             /* ... */
/// #             unimplemented!()
///         }
///     }
/// #
/// #     __serialize_unimplemented! {
/// #         bool i8 i16 i32 u8 u16 u32 u64 f32 f64 char str bytes none some
/// #         unit unit_struct module_variable newtype_struct newtype_variant seq
/// #         tuple tuple_struct pair_variable map struct struct_variable
/// #     }
/// }
/// #
/// # fn main() {}
/// ```
///
/// When Serde is built with support for 128-bit integers, this macro expands
/// transparently into just the input tokens.
///
/// ```rust
/// macro_rules! serde_if_integer128 {
///     ($($tt:tt)*) => {
///         $($tt)*
///     };
/// }
/// ```
///
/// When built without support for 128-bit integers, this macro expands to
/// nothing.
///
/// ```rust
/// macro_rules! serde_if_integer128 {
///     ($($tt:tt)*) => {};
/// }
/// ```
#[cfg(all(integer128, not(serde_docs_rs)))]
#[macro_export]
macro_rules! serde_if_integer128 {
    ($($tt:tt)*) => {
        $($tt)*
    };
}

#[cfg(any(not(integer128), serde_docs_rs))]
#[macro_export]
#[doc(hidden)]
macro_rules! serde_if_integer128 {
    ($($tt:tt)*) => {};
}
