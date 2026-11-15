# Manual page. The roff source is installed as is; formatted copies
# (text, PostScript, PDF) are produced only when groff is available.
install(FILES man/dagonfs.1 DESTINATION "${MANDIR}/man1")
install(CODE "file(CREATE_LINK dagonfs.1 \"\$ENV{DESTDIR}\${CMAKE_INSTALL_PREFIX}/${MANDIR}/man1/DAGonFS_Launcher.1\" SYMBOLIC)")

find_program(GROFF_EXECUTABLE groff)
if(GROFF_EXECUTABLE)
  add_custom_command(OUTPUT dagonfs.txt
                     COMMAND ${CMAKE_COMMAND} -E env GROFF_NO_SGR=1 ${GROFF_EXECUTABLE} -e -T ascii -man
                             ${CMAKE_CURRENT_SOURCE_DIR}/man/dagonfs.1 > dagonfs.txt
                     DEPENDS man/dagonfs.1
                     COMMENT "Formatting the manual page as text")
  add_custom_command(OUTPUT dagonfs.ps
                     COMMAND ${GROFF_EXECUTABLE} -e -T ps -man ${CMAKE_CURRENT_SOURCE_DIR}/man/dagonfs.1 > dagonfs.ps
                     DEPENDS man/dagonfs.1
                     COMMENT "Formatting the manual page as PostScript")
  add_custom_target(manual ALL DEPENDS dagonfs.txt dagonfs.ps)
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/dagonfs.txt" "${CMAKE_CURRENT_BINARY_DIR}/dagonfs.ps" DESTINATION "${DOCDIR}")
  find_program(PS2PDF_EXECUTABLE ps2pdf)
  if(PS2PDF_EXECUTABLE)
    add_custom_command(OUTPUT dagonfs.pdf
                       COMMAND ${PS2PDF_EXECUTABLE} dagonfs.ps dagonfs.pdf
                       DEPENDS dagonfs.ps
                       COMMENT "Converting the manual page to PDF")
    add_custom_target(manual-pdf ALL DEPENDS dagonfs.pdf)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/dagonfs.pdf" DESTINATION "${DOCDIR}")
  endif()
endif()

# Project documentation (Markdown) and figures.
install(DIRECTORY ./ DESTINATION "${DOCDIR}" FILES_MATCHING PATTERN "*.md" PATTERN "man" EXCLUDE)
